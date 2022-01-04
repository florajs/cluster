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
        .then((status) => {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(status));
        })
        .catch((err) => {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end(err.message);
        });
});

// attach @florajs/cluster to our server
worker.attach(httpServer);

worker.on('close', () => {
    worker.log.info('Worker is closing');
    httpServer.close();
});

httpServer.on('listening', () => {
    worker.log.info('Server running on port 3000');
    worker.ready();
});

httpServer.listen(1337);
