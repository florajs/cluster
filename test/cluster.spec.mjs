import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import floraCluster from '../index.js';

describe('cluster', () => {
    it('should be an object', () => {
        assert.equal(typeof floraCluster, 'object');
    });

    it('should expose master and worker objects', () => {
        assert.ok(floraCluster.Master);
        assert.ok(floraCluster.Worker);
    });

    describe('Worker', () => {
        it('should be a function', () => {
            assert.equal(typeof floraCluster.Worker, 'function');
        });

        describe('instance', () => {
            const worker = new floraCluster.Worker();

            it('should expose functions', () => {
                assert.ok(worker.ready);
                assert.ok(worker.attach);
                assert.ok(worker.serverStatus);
                assert.ok(worker.shutdown);
            });
        });
    });
});
