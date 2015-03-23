'use strict';

var bunyan = require('bunyan');
var cluster = require('cluster');
var path = require('path');
var os = require('os');

// Exports

module.exports = {
    run: run,
    reload: reload,
    setConfig: setConfig,
    shutdown: shutdown
};

// State

var config;
var generation = 0;
var isRunning = true;
var currentGenerationForked = false;
var inBeforeReload = false;
var serverStatus = {
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
var serverStatusTimeout = null;

// Logging

var log = null;

/**
 * Run the cluster master
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
 * @api public
 */
function run(opts) {

    // Config

    config = opts;
    if (!config.exec) throw new Error('Must define an "exec" script');
    config.workers = config.workers || os.cpus().length;
    config.startupTimeout = config.startupTimeout || 10000;
    config.shutdownTimeout = config.shutdownTimeout || 30000;

    if (opts.log) {
        log = opts.log.child({'component': 'master'});
    } else {
        log = bunyan.createLogger({name: 'flora-cluster', 'component': 'master'});
    }

    cluster.setupMaster({
        exec: path.resolve(config.exec),
        args: config.args || [],
        silent: config.silent
    });

    cluster.on('fork', onFork);

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

    process.on('SIGHUP', function onSigHup() {
        log.info('received SIGHUP, reloading gracefully');
        reload();
    });

    process.on('uncaughtException', function onUncaughtException(err) {
        log.error(err.stack);
        process.exit(1);
    });

    reload();

    function onFork(worker) {
        worker._floraGeneration = generation;
        worker._floraStatus = 'forked';

        worker.on('message', function onWorkerMessage(message) {
            if (!message.event) return;
            log.trace('message from worker ' + worker.process.pid, message.event);
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

            readjustCluster();
        });

        worker.on('flora::shutdown', function onWorkerShutdown() {
            worker._floraStatus = 'shutdown';

            readjustCluster();
        });

        worker.on('disconnect', function onWorkerDisconnect() {
            worker._floraStatus = 'disconnected';

            readjustCluster();
        });

        worker.on('exit', function onWorkerExit(code, signal) {
            worker._floraStatus = 'exited';

            aggregateServerStatus(worker.fullStatus || {}, serverStatus);

            if (signal) {
                log.warn('worker %d killed by %s', worker.process.pid, signal);
            } else if (code !== 0) {
                log.warn('worker %d exited with error code %d', worker.process.pid, code);
            } else {
                log.info('worker %d exited successfully', worker.process.pid);
            }

            if (worker._florakillTimeout) {
                clearTimeout(worker._floraKillTimeout);
                delete worker._floraKillTimeout;
            }

            readjustCluster();
        });

        worker.on('flora::serverStatus', function onServerStatus() {
            log.trace('worker %d requested serverStatus', worker.process.pid);
            var askWorkerId, askWorker;

            worker._floraNotifyServerStatus = true;

            if (!serverStatusTimeout) {
                serverStatusTimeout = setTimeout(function onServerStatusTimeout() {
                    serverStatusTimeout = null;
                    notifyServerStatus(true);
                }, 1000);
            }

            for (askWorkerId in cluster.workers) {
                askWorker = cluster.workers[askWorkerId];

                askWorker.flora.fullStatusIsUp2date = false;
                askWorker.send({event: 'status'});
            }
        });

        worker.on('flora::status', function onWorkerStatus(message) {
            worker._floraFullStatus = message.status;
            worker._floraFullStatusIsUp2date = true;

            notifyServerStatus();
        });

        setWorkerKillTimeout(worker, 'startup');
    }
}

/**
 * Reload the workers
 *
 * @api public
 */
function reload() {
    inBeforeReload = true;

    if (config.beforeReload && generation > 0) {
        config.beforeReload(function () {
            doReload();
        });
    } else {
        doReload();
    }

    function doReload() {
        inBeforeReload = false;

        generation++;
        currentGenerationForked = false;

        if (generation === 1) {
            serverStatus.status = 'starting';
            log.info('Starting Flora master process at PID ' + process.pid);
        } else {
            serverStatus.status = 'reloading';
            log.info('Gracefully reloading Flora (generation ' + generation + ')');
        }

        readjustCluster();
    }
}

/**
 * Set the cluster config. Merges config values into the existing config
 *
 * @param {Object} opts
 * @api public
 */
function setConfig(opts) {
    for (var name in opts) {
        config[name] = opts[name];
    }

    if (!inBeforeReload) {
        readjustCluster();
    }
}

function readjustCluster() {
    var worker, workerId, i;
    var activeWorkers = 0, runningWorkers = 0;

    // count active and running workers for current generation:
    for (workerId in cluster.workers) {
        worker = cluster.workers[workerId];
        if (worker._floraGeneration !== generation) continue;

        if (worker._floraStatus === 'running' ||
            worker._floraStatus === 'initializing' ||
            worker._floraStatus === 'forked') {
            activeWorkers++;

            if (worker._floraStatus === 'running') {
                runningWorkers++;
            }
        }
    }

    if (serverStatus.status === 'reloading') {
        if (currentGenerationForked && activeWorkers < config.workers) {
            // rollback failing new generation (will be killed automatically afterwards):
            generation--;
            serverStatus.status = 'running';

            log.error('Graceful reload failed - keeping generation ' + generation + ' online');

            readjustCluster();
            return;
        }
    }

    // TODO: do throttling on permanent errors

    if (isRunning) {
        // start missing workers:
        for (i = 0; i < config.workers - activeWorkers; i++) {
            log.info('readjustCluster: forking new worker');
            cluster.fork();
        }

        currentGenerationForked = true;

        // stop workers of current generation if we have too much:
        if (activeWorkers > config.workers) {
            var tooMuchWorkers = activeWorkers - config.workers;

            for (workerId in cluster.workers) {
                if (tooMuchWorkers <= 0) break;

                worker = cluster.workers[workerId];

                if (worker._floraGeneration !== generation) continue;

                shutdownWorker(worker);
                tooMuchWorkers--;
            }
        }
    }

    // if all workers for new generation are up and running, shutdown the old ones -
    // same process if we are shutting down the complete server:
    if (runningWorkers >= config.workers || !isRunning) {
        if (isRunning) {
            serverStatus.status = 'running';
        }

        for (workerId in cluster.workers) {
            worker = cluster.workers[workerId];

            if (worker._floraGeneration === generation && isRunning) continue;

            shutdownWorker(worker);
        }
    }

    notifyServerStatus();
}

function shutdownWorker(worker) {
    if (worker._floraStatus === 'running' ||
        worker._floraStatus === 'initializing' ||
        worker._floraStatus === 'forked') {
        log.info('Shutdown worker ' + worker.process.pid + ' (generation ' + worker._floraGeneration + ')');

        worker._floraStatus = 'shutdownRequested';
        worker.send({event: 'shutdown'});

        setWorkerKillTimeout(worker, 'shutdown');
    }
}

function setWorkerKillTimeout(worker, type) {
    if (worker._floraKillTimeout) {
        clearTimeout(worker._floraKillTimeout);
    }

    // we add one second to the timeouts in master, to give the worker the chance
    // to handle the same timeouts itself before we kill him

    worker._floraKillTimeout = setTimeout(function onWorkerKillTimeout() {
        log.warn('Killing worker ' + worker.process.pid + ' after ' + type + ' timeout by master');
        worker.kill();
    }, config[type + 'Timeout'] + 1000);
}

function notifyServerStatus(timeoutReached) {
    var workerStatusIsUp2date = true;
    var workerId, worker, workerStatus;
    var serverStatusToSend = {}, key;

    if (serverStatusTimeout) {
        clearTimeout(serverStatusTimeout);
        serverStatusTimeout = null;
    }

    for (key in serverStatus) {
        serverStatusToSend[key] = serverStatus[key];
    }

    serverStatusToSend.generation = generation;
    serverStatusToSend.config = config;
    serverStatusToSend.workers = {};

    for (workerId in cluster.workers) {
        worker = cluster.workers[workerId];

        workerStatus = worker._floraFullStatus || {};
        workerStatus.status = worker._floraStatus;
        workerStatus.generation = worker._floraGeneration;

        if (timeoutReached && !worker._floraFullStatusIsUp2date) {
            workerStatus.error = 'Worker did not respond - status outdated';
        }

        aggregateServerStatus(workerStatus, serverStatusToSend);

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
}

function aggregateServerStatus(workerStatus, status) {
    status.connections += workerStatus.connections || 0;
    status.requests += workerStatus.requests || 0;
    status.bytesRead += workerStatus.bytesRead || 0;
    status.bytesWritten += workerStatus.bytesWritten || 0;
}

/**
 * Shutdown the cluster
 *
 * @api public
 */
function shutdown() {
    if (config.beforeShutdown) {
        config.beforeShutdown(function () {
            doShutdown();
        });
    } else {
        doShutdown();
    }

    function doShutdown() {
        isRunning = false;
        serverStatus.status = 'shutdown';

        readjustCluster();
    }
}
