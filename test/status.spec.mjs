import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import Status from '../lib/status.js';

describe('status', () => {
    let status;

    beforeEach(() => (status = new Status()));

    describe('set', () => {
        it('should support key/value', () => {
            status.set('key', 'val');
            assert.equal(status.getStatus().key, 'val');
        });

        it('should support objects', () => {
            status.set({ key1: 'val1', key2: 'val2' });
            assert.equal(status.getStatus().key1, 'val1');
            assert.equal(status.getStatus().key2, 'val2');
        });
    });
});
