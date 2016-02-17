'use strict';

var bunyan = require('bunyan');
var cluster = require('cluster');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Status = require('./status');
var removeValue = require('./remove-value');
var prependListener = require('./prepend-listener');

var isInstantiated = false;

/**
 * Cluster worker.
 *
 * Options:
 * - shutdownTimeout: worker shutdown timeout (default: 30000)
 * - log: bunyan instance for logging (default: new bunyan logger)
 * - httpServer: http.Server instance (optional)
 *
 * @param {Object} opts
 * @api public
 * @constructor
 */
var Worker = module.exports = function (opts) {
    var self = this;

    this.config = opts || {};

    this.config.shutdownTimeout = this.config.shutdownTimeout || 30000;

    if (this.config.log) {
        this.log = opts.log.child({component: 'worker'});
    } else {
        this.log = bunyan.createLogger({name: 'flora-cluster', component: 'worker'});
    }

    this.httpServer = null;
    this.isRunning = true;
    this.activeConnections = [];

    this.status = new Status();
    this.status.set('state', 'initializing');
    this.status.set('startTime', new Date());
    this.status.set('pid', process.pid);
    this.status.set('generation', null); // just for nicer order - set later in master
    this.status.onStatus(function () {
        this.set('state', self.isRunning ? 'running' : 'shutdown');
        this.set('memoryUsage', process.memoryUsage());
    });

    if (isInstantiated) this.log.warn('Worker should be instantiated only once per process');
    isInstantiated = true;

    process.on('message', function onMessage(message) {
        if (!message.event) return;
        self.log.trace(message, 'message from master');
        process.emit('flora::' + message.event, message);
    });

    process.on('flora::shutdown', this.shutdown.bind(this));

    process.on('flora::status', this.sendStatus.bind(this));

    process.on('flora::serverStatus', function onServerStatus(message) {
        serverStatusEvent.emit('serverStatus', message.serverStatus);
    });

    process.on('SIGTERM', function onSigTerm() {
        self.log.info('received SIGTERM, exiting immediately');
        process.exit(0);
    });

    process.on('SIGINT', function onSigInt() {
        self.log.info('received SIGINT, exiting immediately');
        process.exit(0);
    });

    process.on('SIGQUIT', function onSigQuit() {
        self.log.info('received SIGQUIT, shutting down gracefully');
        self.shutdown();
    });

    process.on('uncaughtException', function onUncaughtException(err) {
        self.log.error(err, 'uncaught Exception, shutting down gracefully');
        self.shutdown();
    });

    process.on('unhandledRejection', function onUnhandledRejection(err, promise) {
        self.log.error(err, 'unhandled rejection, shutting down gracefully');
        self.shutdown();
    });

    if (this.config.httpServer) {
        this.attach(this.config.httpServer);
    }
};

util.inherits(Worker, EventEmitter);

// Server status listener

var serverStatusEvent = new EventEmitter();
serverStatusEvent.fetch = function () {
    if (!cluster.isMaster) process.send({event: 'serverStatus'});
};

/**
 * Tell the master that the worker is ready.
 *
 * This function needs to be called before `startupTimeout` is over, otherwise the worker
 * is assumed to failed to start and thus being killed by the master process.
 *
 * @api public
 */
Worker.prototype.ready = function () {
    this.log.debug('sending "ready" event to master');
    if (!cluster.isMaster) process.send({event: 'ready'});
};

/**
 * Attach to an exising HTTP server.
 *
 * This may be necessary after creating the Worker instance, so we can instantiate the
 * http.Server later.
 *
 * @param {http.Server} server
 * @api public
 */
Worker.prototype.attach = function (server) {
    this.httpServer = server;
    prependListener(this.httpServer, 'request', this.onRequest.bind(this));
    prependListener(this.httpServer, 'connection', this.onConnection.bind(this));
    this.httpServer.on('close', this.onCloseServer.bind(this));
};

/**
 * Shutdown the worker
 *
 * @api public
 */
Worker.prototype.shutdown = function () {
    var self = this;

    if (!this.isRunning) return;
    this.isRunning = false;

    if (!this.httpServer) {
        if (!cluster.isMaster) cluster.worker.disconnect();
        return;
    }

    try {
        // make sure we close down within given timeout:
        var shutdownTimeout = setTimeout(function onShutdownTimeout() {
            self.log.info('exit after timeout');
            process.exit(1);
        }, this.config.shutdownTimeout);

        // but don't keep the process open just for that:
        shutdownTimeout.unref();

        // notify master that we are shutting down (so new workers can be spawned immediately):
        if (!cluster.isMaster) process.send({event: 'shutdown'});

        this.emit('close');

        // close idle keep-alive connections:
        this.activeConnections.forEach(function (connection) {
            if (connection.flora.isIdle) {
                connection.destroy();
            }
        });

        this.sendStatus();
    } catch (e) {
        this.log.error(e, 'error on graceful shutdown');
    }
};

Worker.prototype.onRequest = function (request, response) {
    var self = this;

    request.flora = request.flora || {};
    request.flora.startTime = process.hrtime();
    request.flora.state = 'processing';

    request.connection.flora.isIdle = false;
    request.connection.flora.status.increment('requests');
    request.connection.flora.status.set('idleSince', null);

    request.flora.status = request.connection.flora.status.child('request');
    request.flora.status.set('state', null);
    request.flora.status.set('startTime', new Date());
    request.flora.status.onStatus(function () {
        request.flora.status.set('state', request.flora.state);
        request.flora.status.set('method', request.method);
        request.flora.status.set('url', request.url);
        request.flora.status.set('httpVersion', request.httpVersion);
    });

    // TODO: Set different timeout for running requests than for idle keep-alive-connections

    // emulate response.on('end') - when finished processing the request:
    trackResponseEnd(request, response);

    response.on('finish', function onFinishRequest() {
        request.flora.state = 'finished';
        request.flora.status.close();
        request.connection.flora.isIdle = true;
        request.connection.flora.status.set('idleSince', new Date());
        if (request.connection.flora.isClosed) {
            request.connection.flora.status.close();
        }

        if (!self.isRunning) {
            // close keep-alive-connection:
            request.connection.end();
        }
    });

    response.on('close', function onCloseRequest() {
        if (request.flora.state !== 'processing') {
            request.flora.status.close();
            if (request.connection.flora.isClosed) {
                request.connection.flora.status.close();
            }
        }

        request.flora.state = 'aborted';
    });
};

function trackResponseEnd(request, response) {
    function changeStatus(state) {
        if (request.flora.state === 'aborted') {
            // remove connection (parent) from status as it's already closed:
            request.flora.status.parent.close();
        } else {
            request.flora.state = state;
        }
    };

    // inject our "end" method to track "sending" status:
    response._floraOrigEnd = response.end;
    response.end = function floraEnd() {
        changeStatus('sending');
        return this._floraOrigEnd.apply(this, arguments);
    };

    // track if someone pipes to us (no "end" call is guaranteed then):
    response.once('pipe', function () {
        changeStatus('piping');
    });
}

Worker.prototype.onConnection = function (connection) {
    var self = this;

    connection.setNoDelay(true);
    connection.setKeepAlive(true);

    connection.flora = connection.flora || {};
    connection.flora.isIdle = true;

    this.status.increment('connections');

    connection.flora.status = this.status.addChild('activeConnections');
    connection.flora.status.set('state', 'connected');
    connection.flora.status.set('startTime', new Date());
    connection.flora.status.set('remoteAddress', connection.remoteAddress);
    connection.flora.status.set('remotePort', connection.remotePort);
    connection.flora.status.set('idleSince', new Date());
    connection.flora.status.onStatus(function () {
        this.setIncrement('bytesRead', connection.bytesRead);
        this.setIncrement('bytesWritten', connection.bytesWritten);
    });

    this.activeConnections.push(connection);

    connection.on('close', function onCloseConnection() {
        connection.flora.status.setIncrement('bytesRead', connection.bytesRead);
        connection.flora.status.setIncrement('bytesWritten', connection.bytesWritten);
        connection.flora.status.set('state', 'closed');
        if (connection.flora.isIdle) {
            connection.flora.status.close();
        } else {
            connection.flora.isClosed = true;
        }
        removeValue(self.activeConnections, connection);
    });
};

Worker.prototype.onCloseServer = function () {
    this.sendStatus();

    // if all references are closed properly, worker exits here,
    // otherwise the kill-timeout from shutdown() should catch it:
    if (!cluster.isMaster) cluster.worker.disconnect();
};

Worker.prototype.sendStatus = function () {
    var status = this.status.getStatus();

    if (!cluster.isMaster) {
        try {
            process.send({
                event: 'status',
                status: status
            });
        } catch (err) {
            // catch rare "channel closed" errors on shutdown
            this.log.error(err, 'error sending status to master');
        }
    }
};

/**
 * Retrieve the cluster status
 *
 * @param {Function} callback
 * @api public
 */
Worker.prototype.serverStatus = function (callback) {
    if (cluster.isMaster) {
        callback(null, this.status.getStatus());
        return;
    }
    serverStatusEvent.fetch();
    serverStatusEvent.once('serverStatus', function onServerStatus(status) {
        callback(null, status);
    });
};
