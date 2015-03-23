'use strict';

var bunyan = require('bunyan');
var cluster = require('cluster');
var EventEmitter = require('events').EventEmitter;
var removeValue = require('./remove-value');
var prependListener = require('./prepend-listener');

// Exports

module.exports = {
    run: run,
    attach: attach,
    serverStatus: serverStatus,
    shutdown: shutdown
};

// Config

var config;

// State

var httpServer;
var isRunning = true;
var activeConnections = [];
var activeRequests = [];
var workerStatus = {
    status: null,
    startTime: new Date(),
    connections: 0,
    requests: 0,
    bytesRead: 0,
    bytesWritten: 0,
    pid: process.pid,
    generation: null,
    activeConnections: null
};

// Logging

var log = null;

// Server status listener

var serverStatusEvent = new EventEmitter();
serverStatusEvent.fetch = function () {
    if (!cluster.isMaster) process.send({event: 'serverStatus'});
};

/**
 * Run the cluster worker
 *
 * Options:
 * - shutdownTimeout: worker shutdown timeout (default: 30000)
 * - log: bunyan instance for logging
 *
 * @param {Object} opts
 * @api public
 */
function run(opts) {
    config = opts;

    if (opts.log) {
        log = opts.log.child({'component': 'worker'});
    } else {
        log = bunyan.createLogger({name: 'flora-cluster', 'component': 'worker'});
    }

    // Setup process events

    process.on('message', function onMessage(message) {
        if (!message.event) return;
        //console.log('worker/%d: message from master: %s', process.pid, message.event);

        process.emit('flora::' + message.event, message);
    });

    process.on('flora::shutdown', shutdown);

    process.on('flora::status', sendStatus);

    process.on('flora::serverStatus', function onServerStatus(message) {
        serverStatusEvent.emit('serverStatus', message.serverStatus);
    });

    // Setup signals

    process.on('SIGTERM', function onSigTerm() {
        log.info('received SIGTERM, exiting immediately');
        process.exit(0);
    });

    process.on('SIGINT', function onSigInt() {
        log.info('received SIGINT, exiting immediately');
        process.exit(0);
    });

    process.on('SIGQUIT', function onSigQuit() {
        log.info('received SIGQUIT, shutting down gracefully');
        shutdown();
    });

    // Handle uncaughtExceptions

    process.on('uncaughtException', function onUncaughtException(err) {
        log.error(err, 'uncaught Exception, shutting down gracefully');
        shutdown();
    });
}

/**
 * Attach to an exising HTTP server
 *
 * @param {http.Server} server
 * @api public
 */
function attach(server) {
    httpServer = server;
    prependListener(httpServer, 'request', onRequest);
    prependListener(httpServer, 'connection', onConnection);
    httpServer.on('close', onCloseServer);
    // TODO: httpServer.setTimeout(...)
}

/**
 * Shutdown the worker
 *
 * @api public
 */
function shutdown() {
    if (!isRunning) return;

    isRunning = false;

    if (!httpServer) {
        if (!cluster.isMaster) cluster.worker.disconnect();
        return;
    }

    try {
        // make sure we close down within given timeout:
        var shutdownTimeout = setTimeout(function onShutdownTimeout() {
            log.warn('exit after timeout');
            process.exit(1);
        }, config.shutdownTimeout);

        // but don't keep the process open just for that:
        shutdownTimeout.unref();

        // notify master that we are shutting down (so new workers can be spawned immediately):
        if (!cluster.isMaster) process.send({event: 'shutdown'});

        // stop accepting new connections:
        try {
            // TODO: Move outside of cluster via callback?
            httpServer.close();
        } catch (err) {}

        // close idle keep-alive connections:
        activeConnections.forEach(function (connection) {
            if (connection.flora.isIdle) {
                connection.end();
            }
        });

        sendStatus();
    } catch (e) {
        log.error(e, 'error on graceful shutdown');
    }
}

function onRequest(request, response) {
    workerStatus.requests++;
    request.connection.flora.requests++;
    request.connection.flora.isIdle = false;
    request.connection.flora.idleSince = null;

    request.flora = request.flora || {};
    request.flora.status = 'processing';
    request.flora.startTime = new Date();

    // remember connection-info for aborted requests:
    request.flora.connectionInfo = {
        remoteAddress: request.connection.remoteAddress,
        remotePort: request.connection.remotePort,
        localAddress: request.connection.localAddress,
        localPort: request.connection.localPort
    };

    activeRequests.push(request);

    // inject our "end" method to track "sending" status
    var _end = response.end;
    response.end = function () {
        if (request.flora.status === 'aborted') {
            removeValue(activeRequests, request);
        } else {
            request.flora.status = 'sending';
        }
        return _end.apply(this, arguments);
    };

    // TODO: Set different timeout for running requests than for idle keep-alive-connections

    response.on('finish', function onFinishRequest() {
        removeValue(activeRequests, request);

        request.connection.flora.isIdle = true;
        request.connection.flora.idleSince = new Date();
        request.flora.status = 'finished';

        updateStats(request.connection);

        if (!isRunning) {
            // close keep-alive-connection:
            request.connection.end();
        }
    });

    response.on('close', function onCloseRequest() {
        if (request.flora.status === 'sending') {
            removeValue(activeRequests, request);
        }

        request.flora.status = 'aborted';

        updateStats(request.connection);
    });
}

function onConnection(connection) {
    connection.setNoDelay(true);
    connection.setKeepAlive(true);

    workerStatus.connections++;
    connection.flora = connection.flora || {};
    connection.flora.startTime = new Date();
    connection.flora.requests = 0;
    connection.flora.isIdle = true;
    connection.flora.idleSince = connection.flora.startTime;

    activeConnections.push(connection);

    connection.on('close', function onCloseConnection() {
        updateStats(connection);
        removeValue(activeConnections, connection);
    });
}

function onCloseServer() {
    sendStatus();

    // if all references are closed properly, worker exits here,
    // otherwise the kill-timeout from shutdown() should catch it:
    if (!cluster.isMaster) cluster.worker.disconnect();
}

function updateStats(connection) {
    // bytesRead/bytesWritten is per connection, not per request (keep-alive!):
    workerStatus.bytesRead += connection.bytesRead -
        (connection.flora._previousBytesRead || 0);
    workerStatus.bytesWritten += connection.bytesWritten -
        (connection.flora._previousBytesWritten || 0);

    connection.flora._previousBytesRead = connection.bytesRead;
    connection.flora._previousBytesWritten = connection.bytesWritten;
}

function sendStatus() {
    var status = getStatus();

    if (!cluster.isMaster) {
        process.send({
            event: 'status',
            status: status
        });
    }
}

function getStatus() {
    var connections = [];

    activeRequests.forEach(function (request) {
        request.flora._isOrphaned = true;
    });

    activeConnections.forEach(function (connection) {
        var matchingRequest = null;
        activeRequests.forEach(function (request) {
            if (request.connection !== connection) return;

            request.flora._isOrphaned = false;

            matchingRequest = {
                status: request.flora.status,
                startTime: request.flora.startTime,
                method: request.method,
                url: request.url,
                httpVersion: request.httpVersion
            };
        });

        connections.push({
            status: 'connected',
            startTime: connection.flora.startTime,
            remoteAddress: connection.remoteAddress,
            remotePort: connection.remotePort,
            localAddress: connection.localAddress,
            localPort: connection.localPort,
            bytesRead: connection.bytesRead,
            bytesWritten: connection.bytesWritten,
            bufferSize: connection.bufferSize,
            isIdle: connection.flora.isIdle,
            idleSince: connection.flora.idleSince,
            requests: connection.flora.requests,
            request: matchingRequest
        });
    });

    activeRequests.forEach(function (request) {
        if (!request.flora._isOrphaned) return;

        connections.push({
            status: 'closed',
            startTime: request.connection.flora.startTime,
            remoteAddress: request.flora.connectionInfo.remoteAddress,
            remotePort: request.flora.connectionInfo.remotePort,
            localAddress: request.flora.connectionInfo.localAddress,
            localPort: request.flora.connectionInfo.localPort,
            bytesRead: request.connection.bytesRead,
            bytesWritten: request.connection.bytesWritten,
            isIdle: request.connection.flora.isIdle,
            idleSince: request.connection.flora.idleSince,
            requests: request.connection.flora.requests,
            request: {
                status: request.flora.status,
                startTime: request.flora.startTime,
                method: request.method,
                url: request.url,
                httpVersion: request.httpVersion
            }
        });
    });

    var status = {}, key;
    for (key in workerStatus) {
        status[key] = workerStatus[key];
    }

    status.status = isRunning ? 'running' : 'shutdown';
    status.activeConnections = connections;

    return status;
}

/**
 * Retrieve the cluster status
 *
 * @param {Function} callback
 * @api public
 */
function serverStatus(callback) {
    if (cluster.isMaster) {
        callback(null, getStatus());
        return;
    }
    serverStatusEvent.fetch();
    serverStatusEvent.once('serverStatus', function onServerStatus(status) {
        callback(null, status);
    });
}
