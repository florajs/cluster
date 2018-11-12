/* eslint-disable no-console */

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
        console.log('TODO: reloading config here ...');
        master.setConfig({
            workers: 3,
            startupTimeout: 10000,
            shutdownTimeout: 30000
        });
    },
    beforeShutdown: async () => {
        console.log('Shutting down now ...');
    }
});

master.run();
