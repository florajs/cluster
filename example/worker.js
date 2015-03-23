'use strict';

var http = require('http');
var ClusterWorker = require('../').Worker;

var worker = new ClusterWorker({
    shutdownTimeout: 30000
});

// http server
var server = http.createServer(function (req, res) {
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
worker.attach(server);

worker.run();

server.on('listening', function () {
    console.log('Server running at http://127.0.0.1:1337/ - PID ' + process.pid);
});

server.listen(1337);
