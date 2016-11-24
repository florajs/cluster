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

    beforeReload: (callback) => {
        console.log('TODO: reloading config here ...');
        master.setConfig({
            workers: 3,
            startupTimeout: 10000,
            shutdownTimeout: 30000
        });

        callback();
    },
    beforeShutdown: (callback) => {
        console.log('Shutting down now ...');
        callback();
    }
});

master.run();
