'use strict';

const cluster = require('cluster');
const path = require('path');
const os = require('os');
const { EventEmitter } = require('events');
const nullLogger = require('abstract-logging');
const Status = require('./status');

nullLogger.child = () => nullLogger;

/**
 * Cluster master.
 */
class Master extends EventEmitter {
    /**
     * Constructor.
     *
     * @param {object} [options]
     * @param {string} options.exec the script to exit
     * @param {number} [options.workers] worker count (default: host CPU count)
     * @param {Array} [options.args] arguments passed to workers (default: [])
     * @param {number} [options.startupTimeout] worker startup timeout (default: 10000)
     * @param {number} [options.shutdownTimeout] worker shutdown timeout (default: 30000)
     * @param {boolean} [options.silent] suppress worker output
     * @param {Logger} [options.log] bunyan compatible logger instance
     * @param {function} [options.beforeReload] executed before the master is reloaded (async)
     * @param {function} [options.beforeShutdown] executed before the master is shut down (async)
     */
    constructor(options = {}) {
        super();

        this.config = options;
        if (!this.config.exec) throw new Error('Must define an "exec" script');
        this.config.workers = this.config.workers || os.cpus().length;
        this.config.startupTimeout = this.config.startupTimeout || 10000;
        this.config.shutdownTimeout = this.config.shutdownTimeout || 30000;

        this.log = options.log ? options.log.child({ component: 'master' }) : nullLogger;

        this.log.trace('initializing Master');

        this.state = 'initializing';
        this.isRunning = true;
        this.generation = 0;
        this.currentGenerationForked = false;
        this.inBeforeReload = false;

        const status = new Status();
        this.status = status;
        status.set('state', null);
        status.set('startTime', new Date());
        status.set('generation', null);
        status.setIncrement('workerCrashes', 0);
        status.onStatus(() => {
            status.set('state', this.state);
            status.set('generation', this.generation);
        });

        const masterStatus = status.child('master');
        this.masterStatus = masterStatus;
        masterStatus.set('pid', process.pid);
        masterStatus.onStatus(() => {
            masterStatus.set('memoryUsage', process.memoryUsage());
        });

        this.serverStatusTimeout = null;
        this.serverStatusListeners = [];
    }

    /**
     * Run the cluster master.
     */
    run() {
        cluster.setupMaster({
            exec: path.resolve(this.config.exec),
            args: this.config.args || [],
            silent: this.config.silent
        });

        // Setup signals

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

        process.on('SIGHUP', () => {
            this.log.info('received SIGHUP, reloading gracefully');
            this.reload();
        });

        process.on('uncaughtException', (err) => {
            this.log.error(err);
            process.exit(1);
        });

        this.reload();

        this.emit('init');
    }

    /**
     * Reload the workers.
     *
     * @returns {Promise}
     */
    async reload() {
        this.inBeforeReload = true;

        if (this.config.beforeReload && this.generation > 0) {
            await this.config.beforeReload();
        }

        this.inBeforeReload = false;

        this.generation++;
        this.currentGenerationForked = false;

        if (this.generation === 1) {
            this.state = 'starting';
            this.log.info('Starting Flora master process at PID ' + process.pid);
        } else {
            this.state = 'reloading';
            this.log.info('Gracefully reloading Flora (generation ' + this.generation + ')');
        }

        this.readjustCluster();
    }

    /**
     * Set the cluster config. Merges config values into the existing config
     *
     * @param {Object} opts
     */
    setConfig(opts) {
        Object.assign(this.config, opts);

        if (!this.inBeforeReload) {
            this.readjustCluster();
        }
    }

    readjustCluster() {
        let activeWorkers = 0;
        let runningWorkers = 0;

        // count active and running workers for current generation:
        Object.entries(cluster.workers).forEach(([, worker]) => {
            if (worker.flora.generation !== this.generation) return;

            if (['running', 'initializing', 'forked'].includes(worker.flora.state)) {
                activeWorkers++;

                if (worker.flora.state === 'running') {
                    runningWorkers++;
                }
            }
        });

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
            for (let i = 0; i < this.config.workers - activeWorkers; i++) {
                this.log.info('readjustCluster: forking new worker');
                this.onFork(cluster.fork());
            }

            this.currentGenerationForked = true;

            // stop workers of current generation if we have too much:
            if (activeWorkers > this.config.workers) {
                let tooMuchWorkers = activeWorkers - this.config.workers;

                Object.entries(cluster.workers).forEach(([, worker]) => {
                    if (tooMuchWorkers <= 0) return;
                    if (worker.flora.generation !== this.generation) return;

                    this.shutdownWorker(worker);
                    tooMuchWorkers--;
                });
            }
        }

        // if all workers for new generation are up and running, shutdown the old ones -
        // same process if we are shutting down the complete server:
        if (runningWorkers >= this.config.workers || !this.isRunning) {
            if (this.isRunning) this.state = 'running';

            Object.entries(cluster.workers).forEach(([, worker]) => {
                if (worker.flora.generation === this.generation && this.isRunning) return;
                this.shutdownWorker(worker);
            });
        }

        // avoid waiting for stale workers for its status
        process.nextTick(() => this.notifyServerStatus());
    }

    onFork(worker) {
        this.log.debug('worker %d was forked', worker.process.pid);

        worker.flora = worker.flora || {};
        worker.flora.state = 'forked';
        worker.flora.generation = this.generation;
        worker.flora.status = this.status.addChild('workers');
        worker.flora.status.onStatus(() => {
            worker.flora.status.set('state', worker.flora.state);
            worker.flora.status.set('generation', worker.flora.generation);
        });

        worker.on('message', (message) => {
            if (!message.event) return;
            this.log.trace(message, 'message from worker %d', worker.process.pid);
            worker.emit('flora::' + message.event, message);
        });

        worker.on('online', () => {
            this.log.debug('worker %d is online', worker.process.pid);
            if (worker.flora.state === 'forked') {
                worker.flora.state = 'initializing';
            }
        });

        worker.on('flora::ready', () => {
            this.log.debug('worker %d claims to be ready', worker.process.pid);
            if (worker.flora.state === 'initializing') {
                worker.flora.state = 'running';

                if (worker.flora._killTimeout) {
                    clearTimeout(worker.flora._killTimeout);
                    delete worker.flora._killTimeout;
                }
            }

            this.readjustCluster();
        });

        worker.on('flora::shutdown', () => {
            this.log.debug('worker %d is shutting down', worker.process.pid);
            worker.flora.state = 'shutdown';

            this.readjustCluster();
        });

        worker.on('disconnect', () => {
            this.log.debug('worker %d disconnected', worker.process.pid);
            worker.flora.state = 'disconnected';

            this.readjustCluster();
        });

        worker.on('exit', (code, signal) => {
            worker.flora.state = 'exited';
            worker.flora.status.close();

            if (signal) {
                this.status.increment('workerCrashes');
                this.log.warn('worker %d killed by %s', worker.process.pid, signal);
            } else if (code !== 0) {
                this.status.increment('workerCrashes');
                this.log.warn('worker %d exited with error code %d', worker.process.pid, code);
            } else {
                this.log.info('worker %d exited successfully', worker.process.pid);
            }

            if (worker.flora._killTimeout) {
                clearTimeout(worker.flora._killTimeout);
                delete worker.flora._killTimeout;
            }

            this.readjustCluster();
        });

        worker.on('flora::serverStatus', () => {
            this.log.trace('worker %d requested serverStatus', worker.process.pid);
            worker.flora._notifyServerStatus = true;

            this.serverStatus();
        });

        worker.on('flora::status', (message) => {
            worker.flora.status.setStatus(message.status);
            worker.flora._fullStatusIsUp2date = true;

            this.notifyServerStatus();
        });

        this.setWorkerKillTimeout(worker, 'startup');
    }

    shutdownWorker(worker) {
        if (['running', 'initializing', 'forked'].includes(worker.flora.state)) {
            this.log.info('Shutdown worker %d (generation %d)', worker.process.pid, worker.flora.generation);

            worker.flora.state = 'shutdownRequested';
            worker.send({ event: 'shutdown' });

            this.setWorkerKillTimeout(worker, 'shutdown');
        }
    }

    setWorkerKillTimeout(worker, type) {
        if (worker.flora._killTimeout) clearTimeout(worker.flora._killTimeout);

        // we add one second to the timeouts in master, to give the worker the chance
        // to handle the same timeouts itself before we kill him

        worker.flora._killTimeout = setTimeout(
            () => {
                this.log.warn('Killing worker ' + worker.process.pid + ' after ' + type + ' timeout by master');
                worker.kill();
            },
            this.config[type + 'Timeout'] + 1000
        );
    }

    notifyServerStatus(timeoutReached) {
        let workerStatusIsUp2date = true;

        Object.entries(cluster.workers).forEach(([, worker]) => {
            if (!worker.flora._fullStatusIsUp2date) {
                workerStatusIsUp2date = false;
                if (timeoutReached) {
                    worker.flora.status.set('error', 'Worker did not respond - status outdated');
                }
            }
        });

        if (workerStatusIsUp2date || timeoutReached) {
            if (this.serverStatusTimeout) {
                clearTimeout(this.serverStatusTimeout);
                this.serverStatusTimeout = null;
            }

            const serverStatus = this.status.getStatus();
            this.serverStatusListeners.forEach((fn) => fn(serverStatus));
            this.serverStatusListeners = [];

            Object.entries(cluster.workers).forEach(([, worker]) => {
                if (!worker.flora._notifyServerStatus) return;

                worker.send({ event: 'serverStatus', serverStatus });
                delete worker.flora._notifyServerStatus;
            });
        }
    }

    /**
     * Shutdown the cluster.
     *
     * @returns {Promise}
     */
    async shutdown() {
        if (this.config.beforeShutdown) await this.config.beforeShutdown();

        this.isRunning = false;
        this.state = 'shutdown';

        this.emit('shutdown');
        this.readjustCluster();
    }

    /**
     * Retrieve the cluster status.
     *
     * @returns {Promise}
     */
    async serverStatus() {
        if (!this.serverStatusTimeout) {
            this.serverStatusTimeout = setTimeout(() => {
                this.serverStatusTimeout = null;
                this.notifyServerStatus(true);
            }, 1000);
        }

        Object.entries(cluster.workers).forEach(([, askWorker]) => {
            if (!askWorker.isConnected()) {
                askWorker.flora._fullStatusIsUp2date = true;
                askWorker.flora.status.set(
                    'error',
                    'Worker already disconnected IPC channel, but is still running - status outdated'
                );
                return;
            }

            try {
                askWorker.flora._fullStatusIsUp2date = false;
                askWorker.send({ event: 'status' });
            } catch (err) {
                // catch rare "channel closed" errors on worker shutdown
                this.log.error(err, 'Error requesting status from worker');

                // don't wait for it:
                askWorker.flora._fullStatusIsUp2date = true;
                askWorker.flora.status.set('error', 'Error requesting status from worker - status outdated');
            }
        });

        return new Promise((resolve) => {
            this.serverStatusListeners.push((status) => resolve(status));
        });
    }
}

module.exports = Master;
