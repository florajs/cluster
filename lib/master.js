'use strict';

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

/**
 * Run the cluster master
 *
 * Options:
 * - exec: the script to exit
 * - workers: worker count (default: host CPU count)
 * - args: arguments passed to workers (default: null)
 * - startupTimeout: how long the master waits for worker startup (default: 10000)
 * - shutdownTimeout: how long the master waits for worker shutdown (default: 30000)
 * - silent: suppress worker output
 * - beforeReload: function that is executed before the master is reloaded
 * - beforeShutdown: unction that is executed before the master is shut down
 *
 * @param {Object} options
 * @api public
 */
function run(opts) {

    // Config

    config = opts;
    if (!config.exec) throw new Error("Must define an 'exec' script");
    config.workers = config.workers || os.cpus().length;
    config.startupTimeout = config.startupTimeout || 10000;
    config.shutdownTimeout = config.shutdownTimeout || 30000;

    cluster.setupMaster({
        exec: path.resolve(config.exec),
        args: config.args,
        silent: config.silent
    });

    cluster.on('fork', onFork);

    // Setup signals

    process.on('SIGTERM', function onSigTerm() {
        console.info('master: received SIGTERM, exiting immediately');
        process.exit(0);
    });

    process.on('SIGINT', function onSigInt() {
        console.info('master: received SIGINT, exiting immediately');
        process.exit(0);
    });

    process.on('SIGQUIT', function onSigQuit() {
        console.info('master: received SIGQUIT, shutting down gracefully');
        shutdown();
    });

    process.on('SIGHUP', function onSigHup() {
        console.info('master: received SIGHUP, reloading gracefully');
        reload();
    });

    process.on('uncaughtException', function onUncaughtException(err) {
        console.error(err.stack);
        process.exit(1);
    });

    reload();

    function onFork(worker) {
        worker.generation = generation;
        worker.status = 'forked';

        worker.on('message', function onWorkerMessage(message) {
            if (!message.event) return;
            //console.log('master: message from worker %d: %s', worker.process.pid, message.event);

            worker.emit('flora::' + message.event, message);
        });

        worker.on('online', function onWorkerOnline() {
            if (worker.status === 'forked') {
                worker.status = 'initializing';
            }
        });

        worker.on('listening', function onWorkerListening() {
            if (worker.status === 'initializing') {
                worker.status = 'running';

                if (worker.killTimeout) {
                    clearTimeout(worker.killTimeout);
                    delete worker.killTimeout;
                }
            }

            readjustCluster();
        });

        worker.on('flora::shutdown', function onWorkerShutdown() {
            worker.status = 'shutdown';

            readjustCluster();
        });

        worker.on('disconnect', function onWorkerDisconnect() {
            worker.status = 'disconnected';

            readjustCluster();
        });

        worker.on('exit', function onWorkerExit(code, signal) {
            worker.status = 'exited';

            aggregateServerStatus(worker.fullStatus || {}, serverStatus);

            if (signal) {
                console.log('master: worker %d killed by %s', worker.process.pid, signal);
            } else if (code !== 0) {
                console.log('master: worker %d exited with error code %d', worker.process.pid, code);
            } else {
                console.log('master: worker %d exited successfully', worker.process.pid);
            }

            if (worker.killTimeout) {
                clearTimeout(worker.killTimeout);
                delete worker.killTimeout;
            }

            readjustCluster();
        });

        worker.on('flora::serverStatus', function onServerStatus() {
            //console.log('master: worker %d requested serverStatus', worker.process.pid);
            var askWorkerId, askWorker;

            worker.notifyServerStatus = true;

            if (!serverStatusTimeout) {
                serverStatusTimeout = setTimeout(function onServerStatusTimeout() {
                    serverStatusTimeout = null;
                    notifyServerStatus(true);
                }, 1000);
            }

            for (askWorkerId in cluster.workers) {
                askWorker = cluster.workers[askWorkerId];

                askWorker.fullStatusIsUp2date = false;
                askWorker.send({event: 'status'});
            }
        });

        worker.on('flora::status', function onWorkerStatus(message) {
            worker.fullStatus = message.status;
            worker.fullStatusIsUp2date = true;

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
            console.log('Starting Flora master process at PID ' + process.pid);
        } else {
            serverStatus.status = 'reloading';
            console.log('Gracefully reloading Flora (generation ' + generation + ')');
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
        if (worker.generation !== generation) continue;

        if (worker.status === 'running' || worker.status === 'initializing' || worker.status === 'forked') {
            activeWorkers++;

            if (worker.status === 'running') {
                runningWorkers++;
            }
        }
    }

    if (serverStatus.status === 'reloading') {
        if (currentGenerationForked && activeWorkers < config.workers) {
            // rollback failing new generation (will be killed automatically afterwards):
            generation--;
            serverStatus.status = 'running';

            console.error('Graceful reload failed - keeping generation ' + generation + ' online');

            readjustCluster();
            return;
        }
    }

    // TODO: do throttling on permanent errors

    if (isRunning) {
        // start missing workers:
        for (i = 0; i < config.workers - activeWorkers; i++) {
            console.log('readjustCluster: forking new worker');
            cluster.fork();
        }

        currentGenerationForked = true;

        // stop workers of current generation if we have too much:
        if (activeWorkers > config.workers) {
            var tooMuchWorkers = activeWorkers - config.workers;

            for (workerId in cluster.workers) {
                if (tooMuchWorkers <= 0) break;

                worker = cluster.workers[workerId];

                if (worker.generation !== generation) continue;

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

            if (worker.generation === generation && isRunning) continue;

            shutdownWorker(worker);
        }
    }

    notifyServerStatus();
}

function shutdownWorker(worker) {
    if (worker.status === 'running' || worker.status === 'initializing' || worker.status === 'forked') {
        console.log('Shutdown worker ' + worker.process.pid + ' (generation ' + worker.generation + ')');

        worker.status = 'shutdownRequested';
        worker.send({event: 'shutdown'});

        setWorkerKillTimeout(worker, 'shutdown');
    }
}

function setWorkerKillTimeout(worker, type) {
    if (worker.killTimeout) {
        clearTimeout(worker.killTimeout);
    }

    // we add one second to the timeouts in master, to give the worker the chance
    // to handle the same timeouts itself before we kill him

    worker.killTimeout = setTimeout(function onWorkerKillTimeout() {
        console.log('Killing worker ' + worker.process.pid + ' after ' + type + ' timeout by master');
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

        workerStatus = worker.fullStatus || {};
        workerStatus.status = worker.status;
        workerStatus.generation = worker.generation;

        if (timeoutReached && !worker.fullStatusIsUp2date) {
            workerStatus.error = 'Worker did not respond - status outdated';
        }

        aggregateServerStatus(workerStatus, serverStatusToSend);

        serverStatusToSend.workers[workerId] = workerStatus;

        if (!worker.fullStatusIsUp2date) {
            workerStatusIsUp2date = false;
        }
    }

    if (workerStatusIsUp2date || timeoutReached) {
        for (workerId in cluster.workers) {
            worker = cluster.workers[workerId];

            if (!worker.notifyServerStatus) continue;

            worker.send({
                event: 'serverStatus',
                serverStatus: serverStatusToSend
            });

            delete worker.notifyServerStatus;
        }
    }
}

function aggregateServerStatus(workerStatus, serverStatus) {
    serverStatus.connections += workerStatus.connections || 0;
    serverStatus.requests += workerStatus.requests || 0;
    serverStatus.bytesRead += workerStatus.bytesRead || 0;
    serverStatus.bytesWritten += workerStatus.bytesWritten || 0;
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
