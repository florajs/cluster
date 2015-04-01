Flora Cluster
=============

Simple cluster manager module with status tracking for HTTP servers.

Features
--------

- Updates in production with zero downtime - *complete worker code is replaceable without shutdown*
- Self-monitoring process-management - *respawn workers on error (as you would expect)*
- Rollback generation if reload fails - *best chances to have a running version everytime*
- Aggregated status with connection-/request-tracking - *see in realtime what happens in production*
- Status extendible (global counters and per-request infos) - *trace your app in realtime - in production*
- Node.js cluster based implementation


Best practices
--------------

### Startup-Tests

You can perform extended tests in your worker on startup - just exit if something fails. On a graceful
reload, the master process will cancel the reload and shutdown all workers of the new generation.
Reload is assumed to be successful when all workers starts listen()ing. The old generation of workers
is shutdown not before all new workers are up and running.

### Status

Aggregated status over all workers can be requested from inside a worker and from the master process.
So it is possible to integrate the status into your application. Another possibility is to start a
HTTP server inside the master process on a management port for those requests.


Examples
--------

### master.js

The master process will start 3 workers and restarts them on crashes:

```js
var ClusterMaster = require('flora-cluster').Master;

var master = new ClusterMaster({
    exec: require('path').join(__dirname, 'worker.js',
    workers: 3 // defaults to os.cpus().length
});

master.run();
```

### worker.js

```js
var http = require('http');
var ClusterWorker = require('flora-cluster').Worker;

var worker = new ClusterWorker();

var httpServer = http.createServer(function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end('Hello World\n');
});

worker.attach(httpServer);
worker.run();

worker.on('close', function () {
    httpServer.close();
});

httpServer.listen(1337);
```

### server-status

You can retrieve an aggregated status from all workers:

```js
worker.serverStatus(function (err, status) {
    console.log(status);
});
```

### full example

See "example" folder.


License
-------

MIT
