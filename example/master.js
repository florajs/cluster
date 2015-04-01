'use strict';

var path = require('path');
var ClusterMaster = require('../').Master;

var master = new ClusterMaster({
    exec: path.join(__dirname, 'worker.js'),
    workers: 3, // defaults to os.cpus().length

    args: [],
    silent: false,

    startupTimeout: 10000,
    shutdownTimeout: 30000,

    beforeReload: function (callback) {
        console.log('TODO: reloading config here ...');
        master.setConfig({
            workers: 3,
            startupTimeout: 10000,
            shutdownTimeout: 30000
        });

        callback();
    },
    beforeShutdown: function (callback) {
        console.log('Shutting down now ...');
        callback();
    }
});

master.run();
