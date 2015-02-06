'use strict';

var expect = require('chai').expect;

var floraCluster = require('../');

describe('flora-cluster', function () {
    it('should be an object', function () {
        expect(floraCluster).to.be.an('object');
    });

    it('should expose master and worker objects', function () {
        expect(floraCluster).to.have.property('master');
        expect(floraCluster).to.have.property('worker');
    });

    describe('worker', function () {
        it('should be an object', function () {
            expect(floraCluster.worker).to.be.an('object');
        });

        it('should expose functions', function () {
            expect(floraCluster.worker).to.have.property('run');
            expect(floraCluster.worker).to.have.property('attach');
            expect(floraCluster.worker).to.have.property('serverStatus');
            expect(floraCluster.worker).to.have.property('shutdown');
        });
    });
});
