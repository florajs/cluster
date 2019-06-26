'use strict';

const path = require('path');
const { Master } = require('../');

const master = new Master({
    exec: path.join(__dirname, 'worker.js'),
    workers: 3, // defaults to os.cpus().length

    args: [],
    silent: false,

    startupTimeout: 10000,
    shutdownTimeout: 30000,

    beforeReload: async () => {
        master.log.info('beforeReload');
        // e.g. reload configuration from file
        const config = {
            workers: 3,
            startupTimeout: 10000,
            shutdownTimeout: 30000
        };
        master.setConfig(config);
    },
    beforeShutdown: async () => {
        master.log.info('beforeShutdown');
    }
});

master.run();
