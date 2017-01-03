Flora Cluster
=============

[![Build Status](https://travis-ci.org/godmodelabs/flora-cluster.svg?branch=master)](https://travis-ci.org/godmodelabs/flora-cluster)
[![NPM version](https://badge.fury.io/js/flora-cluster.svg)](https://www.npmjs.com/package/flora-cluster)
[![Dependencies](https://img.shields.io/david/godmodelabs/flora-cluster.svg)](https://david-dm.org/godmodelabs/flora-cluster)

Simple cluster manager module with status tracking for HTTP servers.

Flora requires __node v6__ or higher.

Features
--------

- Updates in production with zero downtime - *complete worker code is replaceable without shutdown*
- Self-monitoring process-management - *respawn workers on error (as you would expect)*
- Rollback generation if reload fails - *best chances to have a running version everytime*
- Aggregated status with connection-/request-tracking - *see in realtime what happens in production*
- Status extendible (global counters and per-request infos) - *trace your app in realtime - in production*
- Node.js cluster based implementation


Examples
--------

### master.js

The master process will start 3 workers and restarts them on crashes:

```js
const { Master } = require('flora-cluster');

const master = new ClusterMaster({
    exec: require('path').join(__dirname, 'worker.js',
    workers: 3 // defaults to os.cpus().length
});

master.run();
```

### worker.js

```js
const { createServer } = require('http');
const { Worker } = require('flora-cluster');

const worker = new ClusterWorker();

const httpServer = createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Hello World\n');
});

worker.attach(httpServer);

worker.on('close', () => httpServer.close());

httpServer.listen(1337);

httpServer.on('listening', () => worker.ready());
```

### server-status

You can retrieve an aggregated status from all workers:

```js
worker.serverStatus((err, status) => console.log(status));
```

### full example

See "example" folder.


Best practices
--------------

### Startup-Tests

You can perform extended tests in your worker on startup - just exit if something fails. On a graceful
reload, the master process will cancel the reload and shutdown all workers of the new generation.
Reload is assumed to be successful when all workers called their ready() function. The old generation
of workers is shutdown not before all new workers are up and running.

### Status

Aggregated status over all workers can be requested from inside a worker and from the master process.
So it is possible to integrate the status into your application. Another possibility is to start a
HTTP server inside the master process on a management port for those requests.


License
-------

[MIT](LICENSE)
