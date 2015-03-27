'use strict';

var bunyan = require('bunyan');
var cluster = require('cluster');
var path = require('path');
var os = require('os');

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
 * - beforeReload: function that is executed before the master is reloaded
 * - beforeShutdown: unction that is executed before the master is shut down
 *
 * @param {Object} opts
 * @constructor
 */
var Master = module.exports = function (opts) {
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

    this.generation = 0;
    this.isRunning = true;
    this.currentGenerationForked = false;
    this.inBeforeReload = false;
    this.serverStatus = {
        status: 'initializing',
        startTime: new Date(),
        connections: 0,
        requests: 0,
        bytesRead: 0,
        bytesWritten: 0,
        pid: process.pid,
        generation: null,
        config: null,
        workers: null
    };
    this.serverStatusTimeout = null;
};

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

    cluster.on('fork', onFork);

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

    function onFork(worker) {
        worker._floraGeneration = self.generation;
        worker._floraStatus = 'forked';

        worker.on('message', function onWorkerMessage(message) {
            if (!message.event) return;
            self.log.trace(message, 'message from worker ' + worker.process.pid);
            worker.emit('flora::' + message.event, message);
        });

        worker.on('online', function onWorkerOnline() {
            if (worker._floraStatus === 'forked') {
                worker._floraStatus = 'initializing';
            }
        });

        worker.on('listening', function onWorkerListening() {
            if (worker._floraStatus === 'initializing') {
                worker._floraStatus = 'running';

                if (worker._floraKillTimeout) {
                    clearTimeout(worker._floraKillTimeout);
                    delete worker._floraKillTimeout;
                }
            }

            self.readjustCluster();
        });

        worker.on('flora::shutdown', function onWorkerShutdown() {
            worker._floraStatus = 'shutdown';

            self.readjustCluster();
        });

        worker.on('disconnect', function onWorkerDisconnect() {
            worker._floraStatus = 'disconnected';

            self.readjustCluster();
        });

        worker.on('exit', function onWorkerExit(code, signal) {
            worker._floraStatus = 'exited';

            self.aggregateServerStatus(worker.fullStatus || {}, self.serverStatus);

            if (signal) {
                self.log.warn('worker %d killed by %s', worker.process.pid, signal);
            } else if (code !== 0) {
                self.log.warn('worker %d exited with error code %d', worker.process.pid, code);
            } else {
                self.log.info('worker %d exited successfully', worker.process.pid);
            }

            if (worker._floraKillTimeout) {
                clearTimeout(worker._floraKillTimeout);
                delete worker._floraKillTimeout;
            }

            self.readjustCluster();
        });

        worker.on('flora::serverStatus', function onServerStatus() {
            self.log.trace('worker %d requested serverStatus', worker.process.pid);
            var askWorkerId, askWorker;

            worker._floraNotifyServerStatus = true;

            if (!self.serverStatusTimeout) {
                self.serverStatusTimeout = setTimeout(function onServerStatusTimeout() {
                    self.serverStatusTimeout = null;
                    self.notifyServerStatus(true);
                }, 1000);
            }

            for (askWorkerId in cluster.workers) {
                askWorker = cluster.workers[askWorkerId];

                askWorker._floraFullStatusIsUp2date = false;
                askWorker.send({event: 'status'});
            }
        });

        worker.on('flora::status', function onWorkerStatus(message) {
            worker._floraFullStatus = message.status;
            worker._floraFullStatusIsUp2date = true;

            self.notifyServerStatus();
        });

        self.setWorkerKillTimeout(worker, 'startup');
    }
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
            self.serverStatus.status = 'starting';
            self.log.info('Starting Flora master process at PID ' + process.pid);
        } else {
            self.serverStatus.status = 'reloading';
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
    var worker, workerId, i;
    var activeWorkers = 0, runningWorkers = 0;

    // count active and running workers for current generation:
    for (workerId in cluster.workers) {
        worker = cluster.workers[workerId];
        if (worker._floraGeneration !== this.generation) continue;

        if (worker._floraStatus === 'running' ||
            worker._floraStatus === 'initializing' ||
            worker._floraStatus === 'forked') {
            activeWorkers++;

            if (worker._floraStatus === 'running') {
                runningWorkers++;
            }
        }
    }

    if (this.serverStatus.status === 'reloading') {
        if (this.currentGenerationForked && activeWorkers < this.config.workers) {
            // rollback failing new generation (will be killed automatically afterwards):
            this.generation--;
            this.serverStatus.status = 'running';

            this.log.error('Graceful reload failed - keeping generation ' + this.generation + ' online');

            this.readjustCluster();
            return;
        }
    }

    // TODO: do throttling on permanent errors

    if (this.isRunning) {
        // start missing workers:
        for (i = 0; i < this.config.workers - activeWorkers; i++) {
            this.log.info('readjustCluster: forking new worker');
            cluster.fork();
        }

        this.currentGenerationForked = true;

        // stop workers of current generation if we have too much:
        if (activeWorkers > this.config.workers) {
            var tooMuchWorkers = activeWorkers - this.config.workers;

            for (workerId in cluster.workers) {
                if (tooMuchWorkers <= 0) break;

                worker = cluster.workers[workerId];

                if (worker._floraGeneration !== this.generation) continue;

                this.shutdownWorker(worker);
                tooMuchWorkers--;
            }
        }
    }

    // if all workers for new generation are up and running, shutdown the old ones -
    // same process if we are shutting down the complete server:
    if (runningWorkers >= this.config.workers || !this.isRunning) {
        if (this.isRunning) {
            this.serverStatus.status = 'running';
        }

        for (workerId in cluster.workers) {
            worker = cluster.workers[workerId];

            if (worker._floraGeneration === this.generation && this.isRunning) continue;

            this.shutdownWorker(worker);
        }
    }

    this.notifyServerStatus();
};

Master.prototype.shutdownWorker = function (worker) {
    if (worker._floraStatus === 'running' ||
        worker._floraStatus === 'initializing' ||
        worker._floraStatus === 'forked') {
        this.log.info('Shutdown worker ' + worker.process.pid + ' (generation ' + worker._floraGeneration + ')');

        worker._floraStatus = 'shutdownRequested';
        worker.send({event: 'shutdown'});

        this.setWorkerKillTimeout(worker, 'shutdown');
    }
};

Master.prototype.setWorkerKillTimeout = function (worker, type) {
    var self = this;

    if (worker._floraKillTimeout) {
        clearTimeout(worker._floraKillTimeout);
    }

    // we add one second to the timeouts in master, to give the worker the chance
    // to handle the same timeouts itself before we kill him

    worker._floraKillTimeout = setTimeout(function onWorkerKillTimeout() {
        self.log.warn('Killing worker ' + worker.process.pid + ' after ' + type + ' timeout by master');
        worker.kill();
    }, self.config[type + 'Timeout'] + 1000);
};

Master.prototype.notifyServerStatus = function (timeoutReached) {
    var workerStatusIsUp2date = true;
    var workerId, worker, workerStatus;
    var serverStatusToSend = {}, key;

    if (this.serverStatusTimeout) {
        clearTimeout(this.serverStatusTimeout);
        this.serverStatusTimeout = null;
    }

    for (key in this.serverStatus) {
        serverStatusToSend[key] = this.serverStatus[key];
    }

    serverStatusToSend.generation = this.generation;
    serverStatusToSend.config = this.config;
    serverStatusToSend.workers = {};

    for (workerId in cluster.workers) {
        worker = cluster.workers[workerId];

        workerStatus = worker._floraFullStatus || {};
        workerStatus.status = worker._floraStatus;
        workerStatus.generation = worker._floraGeneration;

        if (timeoutReached && !worker._floraFullStatusIsUp2date) {
            workerStatus.error = 'Worker did not respond - status outdated';
        }

        this.aggregateServerStatus(workerStatus, serverStatusToSend);

        serverStatusToSend.workers[workerId] = workerStatus;

        if (!worker._floraFullStatusIsUp2date) {
            workerStatusIsUp2date = false;
        }
    }

    if (workerStatusIsUp2date || timeoutReached) {
        for (workerId in cluster.workers) {
            worker = cluster.workers[workerId];

            if (!worker._floraNotifyServerStatus) continue;

            worker.send({
                event: 'serverStatus',
                serverStatus: serverStatusToSend
            });

            delete worker._floraNotifyServerStatus;
        }
    }
};

Master.prototype.aggregateServerStatus = function (workerStatus, status) {
    status.connections += workerStatus.connections || 0;
    status.requests += workerStatus.requests || 0;
    status.bytesRead += workerStatus.bytesRead || 0;
    status.bytesWritten += workerStatus.bytesWritten || 0;
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
        self.serverStatus.status = 'shutdown';

        self.readjustCluster();
    }
};
