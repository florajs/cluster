'use strict';

const cluster = require('cluster');
const DgramSocket = require('dgram').Socket;
const { EventEmitter } = require('events');
const { Socket, Server } = require('net');
const ChildProcess = require('child_process').ChildProcess;

const bunyan = require('bunyan');

const Status = require('./status');
const removeValue = require('./remove-value');
const prependListener = require('./prepend-listener');

let isInstantiated = false;

// Server status listener
const serverStatusEvent = new EventEmitter();
serverStatusEvent.fetch = () => {
    if (!cluster.isMaster) process.send({ event: 'serverStatus' });
};

function trackResponseEnd(request, response) {
    function changeStatus(state) {
        if (request.flora.state === 'aborted') {
            // remove connection (parent) from status as it's already closed:
            request.flora.status.parent.close();
        } else {
            request.flora.state = state;
        }
    }

    // inject our "end" method to track "sending" status:
    response._floraOrigEnd = response.end;
    response.end = (...args) => {
        changeStatus('sending');
        return response._floraOrigEnd(...args);
    };

    // track if someone pipes to us (no "end" call is guaranteed then):
    response.once('pipe', () => changeStatus('piping'));
}

/**
 * Cluster worker.
 */
class Worker extends EventEmitter {
    /**
     * Constructor.
     *
     * * Options:
     * - shutdownTimeout: worker shutdown timeout (default: 30000)
     * - log: bunyan instance for logging (default: new bunyan logger)
     * - httpServer: http.Server instance (optional)
     *
     * @param {Object} [opts]
     */
    constructor(opts = {}) {
        super();

        this.config = opts;

        this.config.shutdownTimeout = this.config.shutdownTimeout || 30000;

        if (this.config.log) {
            this.log = opts.log.child({ component: 'worker' });
        } else {
            this.log = bunyan.createLogger({ name: 'flora-cluster', component: 'worker' });
        }

        this.httpServer = null;
        this.isRunning = true;
        this.activeConnections = [];

        this.status = new Status();
        this.status.set('state', 'initializing');
        this.status.set('startTime', new Date());
        this.status.set('pid', process.pid);
        this.status.set('generation', null); // just for nicer order - set later in master
        this.status.onStatus(() => {
            this.status.set('state', this.isRunning ? 'running' : 'shutdown');
            this.status.set('memoryUsage', process.memoryUsage());
        });

        if (isInstantiated) this.log.warn('Worker should be instantiated only once per process');
        isInstantiated = true;

        process.on('message', (message) => {
            if (!message.event) return;
            this.log.trace(message, 'message from master');
            process.emit(`flora::${message.event}`, message);
        });

        process.on('flora::shutdown', this.shutdown.bind(this));

        process.on('flora::status', this.sendStatus.bind(this));

        process.on('flora::serverStatus', (message) => {
            serverStatusEvent.emit('serverStatus', message.serverStatus);
        });

        process.on('SIGTERM', () => {
            this.log.info('received SIGTERM, exiting immediately');
            process.exit(0);
        });

        process.on('SIGINT', () => {
            this.log.info('received SIGINT, exiting immediately');
            process.exit(0);
        });

        process.on('SIGQUIT', () => {
            this.log.info('received SIGQUIT, shutting down gracefully');
            this.shutdown();
        });

        process.on('uncaughtException', (err) => {
            this.log.error(err, 'uncaught exception, shutting down gracefully');
            process.exitCode = 1;
            this.shutdown();
        });

        process.on('unhandledRejection', (err /* , promise */) => {
            this.log.error(err, 'unhandled rejection, shutting down gracefully');
            process.exitCode = 1;
            this.shutdown();
        });

        if (this.config.httpServer) {
            this.attach(this.config.httpServer);
        }
    }

    /**
     * Tell the master that the worker is ready.
     *
     * This function needs to be called before `startupTimeout` is over, otherwise the worker
     * is assumed to failed to start and thus being killed by the master process.
     */
    ready() {
        if (!cluster.isMaster) {
            this.log.debug('sending "ready" event to master');
            this.send({ event: 'ready' });
        }
    }

    /**
     * Attach to an exising HTTP server.
     *
     * This may be necessary after creating the Worker instance, so we can instantiate the
     * http.Server later.
     *
     * @param {http.Server} server
     */
    attach(server) {
        this.httpServer = server;
        prependListener(this.httpServer, 'request', this.onRequest.bind(this));
        prependListener(this.httpServer, 'connection', this.onConnection.bind(this));
        this.httpServer.on('close', this.onCloseServer.bind(this));
    }

    /**
     * Shutdown the worker
     */
    shutdown() {
        if (!this.isRunning) return;
        this.isRunning = false;

        if (!this.httpServer) {
            if (!cluster.isMaster) cluster.worker.disconnect();
            return;
        }

        try {
            // make sure we close down within given timeout:
            const shutdownTimeout = setTimeout(() => {
                this.logActiveHandles();
                this.log.warn('exit after timeout');
                setTimeout(() => {
                    process.exit(2);
                }, 100); // give async logging a chance before hard exit
            }, this.config.shutdownTimeout);

            // but don't keep the process open just for that:
            shutdownTimeout.unref();

            // notify master that we are shutting down (so new workers can be spawned immediately):
            if (!cluster.isMaster) this.send({ event: 'shutdown' });

            this.emit('close');

            // close keep-alive-connection (and do not wait for FIN ACK from client):
            this.activeConnections.forEach((connection) => {
                if (connection.flora.isIdle) connection.destroy();
            });

            this.sendStatus();
        } catch (e) {
            this.log.error(e, 'error on graceful shutdown');
        }
    }

    logActiveHandles() {
        if (typeof process._getActiveHandles !== 'function') {
            this.log.debug("Can't check for active handles - process._getActiveHandles not available");
            return;
        }

        const whitelist = [];
        if (process.stdout) whitelist.push(process.stdout);
        if (process.stderr) whitelist.push(process.stderr);
        if (cluster.worker && cluster.worker.suicide && process._channel) {
            whitelist.push(process._channel); // cluster IPC pipe
        }

        const Timer = process.binding('timer_wrap').Timer; // internal node interface!
        const handles = process._getActiveHandles(); // internal node interface!

        const handleInfos = [];
        handles.forEach((handle) => {
            let name = typeof handle;
            let description = null;

            if (whitelist.indexOf(handle) !== -1) return;

            if (handle === null) {
                name = 'null-handle';
                description = 'wtf?!';
            } else if (handle && typeof handle === 'object') {
                if (handle.constructor && handle.constructor.name) {
                    name = handle.constructor.name;
                }

                if (handle instanceof Socket) {
                    description =
                        handle.localAddress +
                        ':' +
                        handle.localPort +
                        ' -> ' +
                        handle.remoteAddress +
                        ':' +
                        handle.remotePort +
                        (handle.destroyed ? ' (destroyed)' : '');
                } else if (handle instanceof DgramSocket) {
                    name = 'dgram.Socket';
                    description = 'type: ' + handle.type;
                } else if (handle instanceof Server) {
                    const { address, port } = handle.address();
                    description = `${address}:${port}`;
                } else if (handle instanceof Timer) {
                    let timer = handle;
                    // eslint-disable-next-line no-cond-assign
                    while ((timer = timer._idleNext)) {
                        if (timer === handle) break;
                        if (timer._onTimeout) description = 'Timeout: ' + timer._idleTimeout;
                    }
                } else if (handle instanceof ChildProcess) {
                    description = 'PID: ' + handle.pid;
                } else {
                    description = `{${Object.keys(handle).join(', ')}}`;
                }
            }

            handleInfos.push({ name, description });
        });

        handleInfos.forEach((handleInfo) => {
            this.log.warn(`Active handle: ${handleInfo.name}` + (handleInfo.description ? ` (${handleInfo.description})` : ''));
        });
    }

    onRequest(request, response) {
        request.flora = request.flora || {};
        request.flora.startTime = process.hrtime();
        request.flora.state = 'processing';

        request.connection.flora.isIdle = false;
        request.connection.flora.status.increment('requests');
        request.connection.flora.status.set('idleSince', null);

        const status = request.connection.flora.status.child('request');
        request.flora.status = status;

        status.set('state', null);
        status.set('startTime', new Date());
        status.onStatus(() => {
            status.set('state', request.flora.state);
            status.set('method', request.method);
            status.set('url', request.url);
            status.set('httpVersion', request.httpVersion);
        });

        // TODO: Set different timeout for running requests than for idle keep-alive-connections

        // emulate response.on('end') - when finished processing the request:
        trackResponseEnd(request, response);

        response.on('finish', () => {
            request.flora.state = 'finished';
            status.close();
            request.connection.flora.isIdle = true;
            request.connection.flora.status.set('idleSince', new Date());
            if (request.connection.flora.isClosed) {
                request.connection.flora.status.close();
            }

            if (!this.isRunning) {
                // close keep-alive-connection (and do not wait for FIN ACK from clients):
                request.connection.destroy();
            }
        });

        response.on('close', () => {
            if (request.flora.state !== 'processing') {
                status.close();
                if (request.connection.flora.isClosed) {
                    request.connection.flora.status.close();
                }
            }

            request.flora.state = 'aborted';
        });
    }

    onConnection(connection) {
        connection.setNoDelay(true);
        connection.setKeepAlive(true);

        connection.flora = connection.flora || {};
        connection.flora.isIdle = true;

        this.status.increment('connections');

        const status = this.status.addChild('activeConnections');
        connection.flora.status = status;

        status.set('state', 'connected');
        status.set('startTime', new Date());
        status.set('remoteAddress', connection.remoteAddress);
        status.set('remotePort', connection.remotePort);
        status.set('idleSince', new Date());
        status.onStatus(() => {
            status.setIncrement('bytesRead', connection.bytesRead);
            status.setIncrement('bytesWritten', connection.bytesWritten);
        });

        this.activeConnections.push(connection);

        connection.on('close', () => {
            status.setIncrement('bytesRead', connection.bytesRead);
            status.setIncrement('bytesWritten', connection.bytesWritten);
            status.set('state', 'closed');
            if (connection.flora.isIdle) {
                status.close();
            } else {
                connection.flora.isClosed = true;
            }
            removeValue(this.activeConnections, connection);
        });
    }

    onCloseServer() {
        this.sendStatus();
        this.log.info('Closing IPC channel - should exit now');

        // if all references are closed properly, worker exits here,
        // otherwise the kill-timeout from shutdown() should catch it:
        if (!cluster.isMaster) cluster.worker.disconnect();
    }

    sendStatus() {
        if (!cluster.isMaster) this.send({ event: 'status', status: this.status.getStatus() });
    }

    send(message) {
        process.send(message, (err) => {
            if (err) {
                // catch IPC channel errors:
                this.log.error(err, 'error sending IPC message to master');
            }
        });
    }

    /**
     * Retrieve the cluster status
     *
     * @returns {Promise}
     */
    async serverStatus() {
        if (cluster.isMaster) return this.status.getStatus();

        return new Promise((resolve) => {
            serverStatusEvent.fetch();
            serverStatusEvent.once('serverStatus', (status) => resolve(status));
        });
    }
}

module.exports = Worker;
