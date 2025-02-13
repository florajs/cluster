import { EventEmitter } from "node:stream";
import { IncomingMessage, ServerResponse, Server } from "node:http";
import { Worker as NodeWorker } from 'node:worker_threads';
import bunyan from "bunyan";

export class Status {
    name: string | null;

    parent: Status | null;

    constructor();

    set(key: string | Record<string, any>, value: any): void;

    increment(name: string, increment?: number, destObj?: Record<string, any>, aggregates?: Record<string, string>): void;

    setIncrement(name: string, value: any): void;

    onStatus(callback: () => void): void;

    child(name: string): Status;

    addChild(name: string): Status;

    close(): void;

    getStatus(): Record<string, any>;

    setStatus(status: Record<string, any>): void;
}

type WorkerOptions = {
    shutdownTimeout?: number,
    log?: bunyan,
    httpServer?: Server,
};

interface WorkerEventMap {
    close: [];
}

export class Worker extends EventEmitter<WorkerEventMap> {
    status: Status;

    constructor(opts?: WorkerOptions);

    /**
     * Tell the master that the worker is ready.
     *
     * This function needs to be called before `startupTimeout` is over, otherwise the worker
     * is assumed to failed to start and thus being killed by the master process.
     */
    ready(): void;

    /**
     * Attach to an exising HTTP server.
     *
     * This may be necessary after creating the Worker instance, so we can instantiate the
     * http.Server later.
     */
    attach(server: Server): void;

    /**
     * Shutdown the worker
     */
    shutdown(): void;

    logActiveHandles(): void;

    onRequest(request: IncomingMessage, response: ServerResponse): void;

    onConnection(connection: IncomingMessage['connection']): void;

    onCloseServer(): void;

    sendStatus(): void;

    send(message: unknown): void;

    /**
     * Retrieve the cluster status
     */
    serverStatus(): Promise<void>;
}

type MasterOptions<WorkerArguments extends Array<unknown> = Array<unknown>> = {
    exec: string,
    /**
     * @default os.cpus().length
     */
    workers?: number,
    args?: WorkerArguments,
    startupTimeout?: number,
    shutdownTimeout?: number,
    silent?: boolean,
    logger?: bunyan,
    beforeReload?: () => Promise<void>,
    beforeShutdown?: () => Promise<void>,
};

interface MasterEventMap {
    init: [];
    shutdown: [];
}

export class Master<WorkerArguments extends Array<unknown> = Array<unknown>> extends EventEmitter<MasterEventMap> {
    constructor(opts?: MasterOptions<WorkerArguments>);

    run(): void;

    reload(): Promise<void>;

    /**
     * Set the cluster config. Merges config values into the existing config
     */
    setConfig(opts: Record<string, unknown>): void;

    readjustCluster(): void;

    onFork(worker: NodeWorker): void;

    shutdownWorker(worker: NodeWorker): void;

    setWorkerKillTimeout(worker: NodeWorker, type: string): void;

    notifyServerStatus(timeoutReached: boolean): void;

    /**
     * Shutdown the cluster.
     */
    shutdown(): Promise<void>;

    /**
     * Retrieve the cluster status.
     */
    serverStatus(): Promise<void>;
}
