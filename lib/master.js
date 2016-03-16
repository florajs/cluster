'use strict';

var bunyan = require('bunyan');
var cluster = require('cluster');
var path = require('path');
var os = require('os');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Status = require('./status');

/**
 * Cluster master.
 *
 * Options:
 * - exec: the script to exit
 * - workers: worker count (default: host CPU count)
 * - args: arguments passed to workers (default: [])
 * - startupTimeout: how long the master waits for worker startup (default: 10000)
 * - shutdownTimeout: how long the master waits for worker shutdown (default: 30000)
 * - silent: suppress worker output
 * - log: bunyan instance for logging
 * - beforeReload: function that is executed before the master is reloaded (async, with callback)
 * - beforeShutdown: function that is executed before the master is shut down (async, with callback)
 *
 * @param {Object} opts
 * @constructor
 */
var Master = module.exports = function (opts) {
    var self = this;

    this.config = opts || {};
    if (!this.config.exec) throw new Error('Must define an "exec" script');
    this.config.workers = this.config.workers || os.cpus().length;
    this.config.startupTimeout = this.config.startupTimeout || 10000;
    this.config.shutdownTimeout = this.config.shutdownTimeout || 30000;

    if (opts.log) {
        this.log = opts.log.child({'component': 'master'});
    } else {
        this.log = bunyan.createLogger({name: 'flora-cluster', 'component': 'master'});
    }

    this.log.trace('initializing Master');

    this.state = 'initializing';
    this.isRunning = true;
    this.generation = 0;
    this.currentGenerationForked = false;
    this.inBeforeReload = false;

    this.status = new Status();
    this.status.set('state', null);
    this.status.set('startTime', new Date());
    this.status.set('pid', process.pid);
    this.status.set('generation', null);
    this.status.setIncrement('workerCrashes', 0);
    this.status.onStatus(function () {
        this.set('state', self.state);
        this.set('generation', self.generation);
    });

    this.serverStatusTimeout = null;
    this.serverStatusCallbacks = [];
};

util.inherits(Master, EventEmitter);

/**
 * Run the cluster master.
 *
 * @api public
 */
Master.prototype.run = function () {
    var self = this;

    cluster.setupMaster({
        exec: path.resolve(this.config.exec),
        args: this.config.args || [],
        silent: this.config.silent
    });

    // Setup signals

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

    process.on('SIGHUP', function onSigHup() {
        self.log.info('received SIGHUP, reloading gracefully');
        self.reload();
    });

    process.on('uncaughtException', function onUncaughtException(err) {
        self.log.error(err);
        process.exit(1);
    });

    this.reload();

    this.emit('init');
};

/**
 * Reload the workers
 *
 * @api public
 */
Master.prototype.reload = function () {
    var self = this;

    this.inBeforeReload = true;

    if (this.config.beforeReload && this.generation > 0) {
        this.config.beforeReload(function () {
            doReload();
        });
    } else {
        doReload();
    }

    function doReload() {
        self.inBeforeReload = false;

        self.generation++;
        self.currentGenerationForked = false;

        if (self.generation === 1) {
            self.state = 'starting';
            self.log.info('Starting Flora master process at PID ' + process.pid);
        } else {
            self.state = 'reloading';
            self.log.info('Gracefully reloading Flora (generation ' + self.generation + ')');
        }

        self.readjustCluster();
    }
};

/**
 * Set the cluster config. Merges config values into the existing config
 *
 * @param {Object} opts
 * @api public
 */
Master.prototype.setConfig = function (opts) {
    for (var name in opts) {
        this.config[name] = opts[name];
    }

    if (!this.inBeforeReload) {
        this.readjustCluster();
    }
};

Master.prototype.readjustCluster = function () {
    var worker, workerId, i, self = this;
    var activeWorkers = 0, runningWorkers = 0;

    // count active and running workers for current generation:
    for (workerId in cluster.workers) {
        worker = cluster.workers[workerId];
        if (worker.flora.generation !== this.generation) continue;

        if (worker.flora.state === 'running' ||
            worker.flora.state === 'initializing' ||
            worker.flora.state === 'forked') {
            activeWorkers++;

            if (worker.flora.state === 'running') {
                runningWorkers++;
            }
        }
    }

    if (this.state === 'reloading') {
        if (this.currentGenerationForked && activeWorkers < this.config.workers) {
            // rollback failing new generation (will be killed automatically afterwards):
            this.generation--;
            this.state = 'running';

            this.log.error('Graceful reload failed - keeping generation %d online', this.generation);

            this.readjustCluster();
            return;
        }
    }

    // TODO: do throttling on permanent errors

    if (this.isRunning) {
        // start missing workers:
        for (i = 0; i < this.config.workers - activeWorkers; i++) {
            this.log.info('readjustCluster: forking new worker');
            worker = cluster.fork();
            this.onFork(worker);
        }

        this.currentGenerationForked = true;

        // stop workers of current generation if we have too much:
        if (activeWorkers > this.config.workers) {
            var tooMuchWorkers = activeWorkers - this.config.workers;

            for (workerId in cluster.workers) {
                if (tooMuchWorkers <= 0) break;

                worker = cluster.workers[workerId];

                if (worker.flora.generation !== this.generation) continue;

                this.shutdownWorker(worker);
                tooMuchWorkers--;
            }
        }
    }

    // if all workers for new generation are up and running, shutdown the old ones -
    // same process if we are shutting down the complete server:
    if (runningWorkers >= this.config.workers || !this.isRunning) {
        if (this.isRunning) {
            this.state = 'running';
        }

        for (workerId in cluster.workers) {
            worker = cluster.workers[workerId];

            if (worker.flora.generation === this.generation && this.isRunning) continue;

            this.shutdownWorker(worker);
        }
    }

    process.nextTick(function () {
        self.notifyServerStatus(); // avoid waiting for stale workers for its status
    });
};

Master.prototype.onFork = function (worker) {
    var self = this;

    self.log.debug('worker %d was forked', worker.process.pid);

    worker.flora = worker.flora || {};
    worker.flora.state = 'forked';
    worker.flora.generation = self.generation;
    worker.flora.status = self.status.addChild('workers');
    worker.flora.status.onStatus(function () {
        this.set('state', worker.flora.state);
        this.set('generation', worker.flora.generation);
    });

    worker.on('message', function onWorkerMessage(message) {
        if (!message.event) return;
        self.log.trace(message, 'message from worker %d', worker.process.pid);
        worker.emit('flora::' + message.event, message);
    });

    worker.on('online', function onWorkerOnline() {
        self.log.debug('worker %d is online', worker.process.pid);
        if (worker.flora.state === 'forked') {
            worker.flora.state = 'initializing';
        }
    });

    worker.on('flora::ready', function onWorkerReady() {
        self.log.debug('worker %d claims to be ready', worker.process.pid);
        if (worker.flora.state === 'initializing') {
            worker.flora.state = 'running';

            if (worker.flora._killTimeout) {
                clearTimeout(worker.flora._killTimeout);
                delete worker.flora._killTimeout;
            }
        }

        self.readjustCluster();
    });

    worker.on('flora::shutdown', function onWorkerShutdown() {
        self.log.debug('worker %d is shutting down', worker.process.pid);
        worker.flora.state = 'shutdown';

        self.readjustCluster();
    });

    worker.on('disconnect', function onWorkerDisconnect() {
        self.log.debug('worker %d disconnected', worker.process.pid);
        worker.flora.state = 'disconnected';

        self.readjustCluster();
    });

    worker.on('exit', function onWorkerExit(code, signal) {
        worker.flora.state = 'exited';
        worker.flora.status.close();

        if (signal) {
            self.status.increment('workerCrashes');
            self.log.warn('worker %d killed by %s', worker.process.pid, signal);
        } else if (code !== 0) {
            self.status.increment('workerCrashes');
            self.log.warn('worker %d exited with error code %d', worker.process.pid, code);
        } else {
            self.log.info('worker %d exited successfully', worker.process.pid);
        }

        if (worker.flora._killTimeout) {
            clearTimeout(worker.flora._killTimeout);
            delete worker.flora._killTimeout;
        }

        self.readjustCluster();
    });

    worker.on('flora::serverStatus', function onServerStatus() {
        self.log.trace('worker %d requested serverStatus', worker.process.pid);

        worker.flora._notifyServerStatus = true;
        self.serverStatus();
    });

    worker.on('flora::status', function onWorkerStatus(message) {
        worker.flora.status.setStatus(message.status);
        worker.flora._fullStatusIsUp2date = true;

        self.notifyServerStatus();
    });

    self.setWorkerKillTimeout(worker, 'startup');
};

Master.prototype.shutdownWorker = function (worker) {
    if (worker.flora.state === 'running' ||
        worker.flora.state === 'initializing' ||
        worker.flora.state === 'forked') {
        this.log.info('Shutdown worker %d (generation %d)', worker.process.pid, worker.flora.generation);

        worker.flora.state = 'shutdownRequested';
        worker.send({event: 'shutdown'});

        this.setWorkerKillTimeout(worker, 'shutdown');
    }
};

Master.prototype.setWorkerKillTimeout = function (worker, type) {
    var self = this;

    if (worker.flora._killTimeout) {
        clearTimeout(worker.flora._killTimeout);
    }

    // we add one second to the timeouts in master, to give the worker the chance
    // to handle the same timeouts itself before we kill him

    worker.flora._killTimeout = setTimeout(function onWorkerKillTimeout() {
        self.log.warn('Killing worker ' + worker.process.pid + ' after ' + type + ' timeout by master');
        worker.kill();
    }, self.config[type + 'Timeout'] + 1000);
};

Master.prototype.notifyServerStatus = function (timeoutReached) {
    var workerStatusIsUp2date = true;
    var workerId, worker;

    for (workerId in cluster.workers) {
        worker = cluster.workers[workerId];

        if (!worker.flora._fullStatusIsUp2date) {
            workerStatusIsUp2date = false;
            if (timeoutReached) {
                worker.flora.status.set('error', 'Worker did not respond - status outdated');
            }
        }
    }

    if (workerStatusIsUp2date || timeoutReached) {
        if (this.serverStatusTimeout) {
            clearTimeout(this.serverStatusTimeout);
            this.serverStatusTimeout = null;
        }

        var serverStatus = this.status.getStatus();

        var cb;
        while (cb = this.serverStatusCallbacks.shift()) {
            cb(null, serverStatus);
        }

        for (workerId in cluster.workers) {
            worker = cluster.workers[workerId];

            if (!worker.flora._notifyServerStatus) continue;

            worker.send({
                event: 'serverStatus',
                serverStatus: serverStatus
            });

            delete worker.flora._notifyServerStatus;
        }
    }
};

/**
 * Shutdown the cluster
 *
 * @api public
 */
Master.prototype.shutdown = function () {
    var self = this;

    if (this.config.beforeShutdown) {
        this.config.beforeShutdown(function () {
            doShutdown();
        });
    } else {
        doShutdown();
    }

    function doShutdown() {
        self.isRunning = false;
        self.state = 'shutdown';

        self.emit('shutdown');
        self.readjustCluster();
    }
};

/**
 * Retrieve the cluster status
 *
 * @param {Function} callback
 * @api public
 */
Master.prototype.serverStatus = function (callback) {
    var self = this;

    if (!this.serverStatusTimeout) {
        this.serverStatusTimeout = setTimeout(function onServerStatusTimeout() {
            self.serverStatusTimeout = null;
            self.notifyServerStatus(true);
        }, 1000);
    }

    var askWorkerId, askWorker;

    for (askWorkerId in cluster.workers) {
        askWorker = cluster.workers[askWorkerId];

        if (!askWorker.isConnected()) {
            askWorker.flora._fullStatusIsUp2date = true;
            askWorker.flora.status.set('error',
                'Worker already disconnected IPC channel, but is still running - status outdated');
            continue;
        }

        try {
            askWorker.flora._fullStatusIsUp2date = false;
            askWorker.send({event: 'status'});
        } catch (err) {
            // catch rare "channel closed" errors on worker shutdown
            this.log.error(err, 'Error requesting status from worker');

            // don't wait for it:
            askWorker.flora._fullStatusIsUp2date = true;
            askWorker.flora.status.set('error', 'Error requesting status from worker - status outdated');
        }
    }

    if (callback) this.serverStatusCallbacks.push(callback);
};
