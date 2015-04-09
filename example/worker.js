'use strict';

var http = require('http');
var ClusterWorker = require('../').Worker;

var worker = new ClusterWorker({
    shutdownTimeout: 30000
});

// http server
var httpServer = http.createServer(function (req, res) {
    worker.serverStatus(function (err, status) {
        if (err) {
            res.writeHead(500, {'Content-Type': 'text/plain'});
            res.end(err.message);
            return;
        }

        res.writeHead(200, {'Content-Type': 'application/json'});
        res.end(JSON.stringify(status));
    });
});

// attach flora-cluster to our server
worker.attach(httpServer);

worker.on('close', function () {
    console.log('Worker is closing');
    httpServer.close();
});

httpServer.on('listening', function () {
    console.log('Server running at http://127.0.0.1:1337/ - PID ' + process.pid);
    worker.ready();
});

httpServer.listen(1337);
