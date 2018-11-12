/* eslint-disable no-console */

'use strict';

const { createServer } = require('http');
const { Worker } = require('../');

const worker = new Worker({
    shutdownTimeout: 30000
});

// http server
const httpServer = createServer((req, res) => {
    worker
        .serverStatus()
        .then(status => {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(status));
        })
        .catch(err => {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end(err.message);
        });
});

// attach flora-cluster to our server
worker.attach(httpServer);

worker.on('close', () => {
    console.log('Worker is closing');
    httpServer.close();
});

httpServer.on('listening', () => {
    console.log('Server running at http://127.0.0.1:1337/ - PID ' + process.pid);
    worker.ready();
});

httpServer.listen(1337);
