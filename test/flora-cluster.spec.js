'use strict';

const { expect } = require('chai');

const floraCluster = require('../');

describe('flora-cluster', () => {
    it('should be an object', () => {
        expect(floraCluster).to.be.an('object');
    });

    it('should expose master and worker objects', () => {
        expect(floraCluster).to.have.property('Master');
        expect(floraCluster).to.have.property('Worker');
    });

    describe('Worker', () => {
        it('should be a function', () => {
            expect(floraCluster.Worker).to.be.a('function');
        });

        describe('instance', () => {
            const worker = new floraCluster.Worker();

            it('should expose functions', () => {
                expect(worker).to.have.property('ready');
                expect(worker).to.have.property('attach');
                expect(worker).to.have.property('serverStatus');
                expect(worker).to.have.property('shutdown');
            });
        });
    });
});
