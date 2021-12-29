'use strict';

const { expect } = require('chai');

const Status = require('../lib/status');

describe('status', () => {
    let status;

    beforeEach(() => {
        status = new Status();
    });

    describe('set', () => {
        it('should support key/value', () => {
            status.set('key', 'val');
            expect(status.getStatus()).to.have.property('key', 'val');
        });

        it('should support objects', () => {
            status.set({ key1: 'val1', key2: 'val2' });
            expect(status.getStatus()).to.include({ key1: 'val1', key2: 'val2' });
        });
    });
});
