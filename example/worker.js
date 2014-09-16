'use strict';

var http = require('http');
var clusterWorker = require('../').worker;

clusterWorker.run({
    shutdownTimeout: 30000
});

// http server
var server = http.createServer(function (req, res) {
    clusterWorker.serverStatus(function (err, status) {
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
clusterWorker.attach(server);

server.on('listening', function () {
    console.log('Server running at http://127.0.0.1:1337/ - PID ' + process.pid);
});

server.listen(1337);
