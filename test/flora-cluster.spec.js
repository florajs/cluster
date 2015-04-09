'use strict';

var expect = require('chai').expect;

var floraCluster = require('../');

describe('flora-cluster', function () {
    it('should be an object', function () {
        expect(floraCluster).to.be.an('object');
    });

    it('should expose master and worker objects', function () {
        expect(floraCluster).to.have.property('Master');
        expect(floraCluster).to.have.property('Worker');
    });

    describe('Worker', function () {
        it('should be a function', function () {
            expect(floraCluster.Worker).to.be.a('function');
        });

        describe('instance', function () {
            var worker = new floraCluster.Worker();

            it('should expose functions', function () {
                expect(worker).to.have.property('ready');
                expect(worker).to.have.property('attach');
                expect(worker).to.have.property('serverStatus');
                expect(worker).to.have.property('shutdown');
            });
        });
    });
});
