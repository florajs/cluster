'use strict';

var bunyan = require('bunyan');
var cluster = require('cluster');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var removeValue = require('./remove-value');
var prependListener = require('./prepend-listener');

var isInstantiated = false;

/**
 * Cluster worker.
 *
 * Options:
 * - shutdownTimeout: worker shutdown timeout (default: 30000)
 * - log: bunyan instance for logging
 *
 * @param {Object} opts
 * @api public
 * @constructor
 */
var Worker = module.exports = function (opts) {
    this.config = opts || {};

    this.config.shutdownTimeout = this.config.shutdownTimeout || 30000;

    if (this.config.log) {
        this.log = opts.log.child({'component': 'worker'});
    } else {
        this.log = bunyan.createLogger({name: 'flora-cluster', 'component': 'worker'});
    }

    this.httpServer = null;
    this.isRunning = true;
    this.activeConnections = [];
    this.activeRequests = [];
    this.workerStatus = {
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

    if (isInstantiated) this.log.warn('Worker should be instantiated only once per process');
    isInstantiated = true;
};

util.inherits(Worker, EventEmitter);

// Server status listener

var serverStatusEvent = new EventEmitter();
serverStatusEvent.fetch = function () {
    if (!cluster.isMaster) process.send({event: 'serverStatus'});
};

/**
 * Run the cluster worker
 *
 * @api public
 */
Worker.prototype.run = function () {
    var self = this;

    process.on('message', function onMessage(message) {
        if (!message.event) return;
        self.log.trace(message, 'message from master');
        process.emit('flora::' + message.event, message);
    });

    process.on('flora::shutdown', this.shutdown);

    process.on('flora::status', this.sendStatus);

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

    if (this.config.httpServer) {
        this.attach(this.config.httpServer);
    }
};

/**
 * Attach to an exising HTTP server
 *
 * @param {http.Server} server
 * @api public
 */
Worker.prototype.attach = function (server) {
    this.httpServer = server;
    prependListener(this.httpServer, 'request', this.onRequest);
    prependListener(this.httpServer, 'connection', this.onConnection);
    this.httpServer.on('close', this.onCloseServer);
    // TODO: httpServer.setTimeout(...)
};

/**
 * Shutdown the worker
 *
 * @api public
 */
Worker.prototype.shutdown = function () {
    if (!this.isRunning) return;

    this.isRunning = false;

    if (!this.httpServer) {
        if (!cluster.isMaster) cluster.worker.disconnect();
        return;
    }

    try {
        // make sure we close down within given timeout:
        var shutdownTimeout = setTimeout(function onShutdownTimeout() {
            this.log.info('exit after timeout');
            process.exit(1);
        }, this.config.shutdownTimeout);

        // but don't keep the process open just for that:
        shutdownTimeout.unref();

        // notify master that we are shutting down (so new workers can be spawned immediately):
        if (!cluster.isMaster) process.send({event: 'shutdown'});

        // stop accepting new connections:
        try {
            if (this.httpServer) this.httpServer.close();
        } catch (err) {}

        // close idle keep-alive connections:
        this.activeConnections.forEach(function (connection) {
            if (connection.flora.isIdle) {
                connection.end();
            }
        });

        this.sendStatus();
    } catch (e) {
        this.log.error(e, 'error on graceful shutdown');
    }
};

Worker.prototype.onRequest = function (request, response) {
    var self = this;

    this.workerStatus.requests++;
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

    this.activeRequests.push(request);

    // inject our "end" method to track "sending" status
    var _end = response.end;
    response.end = function () {
        if (request.flora.status === 'aborted') {
            removeValue(this.activeRequests, request);
        } else {
            request.flora.status = 'sending';
        }
        return _end.apply(self, arguments);
    };

    // TODO: Set different timeout for running requests than for idle keep-alive-connections

    response.on('finish', function onFinishRequest() {
        removeValue(this.activeRequests, request);

        request.connection.flora.isIdle = true;
        request.connection.flora.idleSince = new Date();
        request.flora.status = 'finished';

        self.updateStats(request.connection);

        if (!this.isRunning) {
            // close keep-alive-connection:
            request.connection.end();
        }
    });

    response.on('close', function onCloseRequest() {
        if (request.flora.status === 'sending') {
            removeValue(this.activeRequests, request);
        }

        request.flora.status = 'aborted';

        self.updateStats(request.connection);
    });
};

Worker.prototype.onConnection = function (connection) {
    var self = this;

    connection.setNoDelay(true);
    connection.setKeepAlive(true);

    this.workerStatus.connections++;
    connection.flora = connection.flora || {};
    connection.flora.startTime = new Date();
    connection.flora.requests = 0;
    connection.flora.isIdle = true;
    connection.flora.idleSince = connection.flora.startTime;

    this.activeConnections.push(connection);

    connection.on('close', function onCloseConnection() {
        self.updateStats(connection);
        removeValue(self.activeConnections, connection);
    });
};

Worker.prototype.onCloseServer = function () {
    this.sendStatus();

    // if all references are closed properly, worker exits here,
    // otherwise the kill-timeout from shutdown() should catch it:
    if (!cluster.isMaster) cluster.worker.disconnect();
};

Worker.prototype.updateStats = function (connection) {
    // bytesRead/bytesWritten is per connection, not per request (keep-alive!):
    this.workerStatus.bytesRead += connection.bytesRead -
        (connection.flora._previousBytesRead || 0);
    this.workerStatus.bytesWritten += connection.bytesWritten -
        (connection.flora._previousBytesWritten || 0);

    connection.flora._previousBytesRead = connection.bytesRead;
    connection.flora._previousBytesWritten = connection.bytesWritten;
};

Worker.prototype.sendStatus = function () {
    var status = this.getStatus();

    if (!cluster.isMaster) {
        process.send({
            event: 'status',
            status: status
        });
    }
};

Worker.prototype.getStatus = function () {
    var self = this;
    var connections = [];

    this.activeRequests.forEach(function (request) {
        request.flora._isOrphaned = true;
    });

    this.activeConnections.forEach(function (connection) {
        var matchingRequest = null;
        self.activeRequests.forEach(function (request) {
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

    this.activeRequests.forEach(function (request) {
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
    for (key in this.workerStatus) {
        status[key] = this.workerStatus[key];
    }

    status.status = this.isRunning ? 'running' : 'shutdown';
    status.activeConnections = connections;

    return status;
};

/**
 * Retrieve the cluster status
 *
 * @param {Function} callback
 * @api public
 */
Worker.prototype.serverStatus = function (callback) {
    if (cluster.isMaster) {
        callback(null, this.getStatus());
        return;
    }
    serverStatusEvent.fetch();
    serverStatusEvent.once('serverStatus', function onServerStatus(status) {
        callback(null, status);
    });
};
