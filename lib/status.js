'use strict';

var removeValue = require('./remove-value');

/**
 * Simple status tracking
 */
var Status = module.exports = function () {
    this._data = {};
    this._aggregates = {};
    this._callbacks = [];

    this.name = null;
    this.parent = null;
    this._children = {};
};

Status.prototype.set = function (name, value) {
    this._data[name] = value;
};

Status.prototype.increment = function (name, increment, destObj, aggregates) {
    (aggregates || this._aggregates)[name] = 'increment';
    destObj = destObj || this._data;

    if (!destObj[name]) destObj[name] = 0;
    destObj[name] += increment || (typeof increment === 'undefined' ? 1 : 0);
};

Status.prototype.setIncrement = function (name, value) {
    this._aggregates[name] = 'increment';
    this._data[name] = value;
};

Status.prototype.onStatus = function (callback) {
    this._callbacks.push(callback);
};

Status.prototype.child = function (name) {
    var childStatus = new Status();
    childStatus.name = name;
    childStatus.parent = this;

    this._children[name] = childStatus;

    return childStatus;
};

Status.prototype.addChild = function (name) {
    var childStatus = new Status();
    childStatus.name = name;
    childStatus.parent = this;

    if (!Array.isArray(this._children[name])) this._children[name] = [];
    this._children[name].push(childStatus);

    return childStatus;
};

Status.prototype.close = function () {
    var name;

    // close/aggregate children first if still open:
    for (name in this._children) {
        if (Array.isArray(this._children[name])) {
            this._children[name].forEach(function (child) {
                if (!child) return;
                child.close();
            });
        } else if (this._children[name]) {
            this._children[name].close();
        }
    }

    if (this.parent) {
        for (name in this._aggregates) {
            this.parent[this._aggregates[name]](name, this._data[name]);
        }

        if (Array.isArray(this.parent._children[this.name])) {
            removeValue(this.parent._children[this.name], this);
        } else {
            this.parent._children[this.name] = null;
        }
        this.parent = null;
    }

    this._callbacks = null; // avoid memleaks
};

Status.prototype.getStatus = function () {
    var status = {}, name, self = this;

    if (Array.isArray(this._callbacks)) {
        this._callbacks.forEach(function (callback) {
            callback.call(self);
        });
    }

    for (name in this._data) {
        status[name] = this._data[name];
    }

    var aggregates = this._aggregates;

    function aggregateChild(child) {
        var childStatus = child.getStatus();

        for (var name in childStatus._aggregates) {
            child.parent[childStatus._aggregates[name]](name, childStatus[name], status, aggregates);
        }
        delete childStatus._aggregates;

        return childStatus;
    }

    var children = {};
    for (name in this._children) {
        if (Array.isArray(this._children[name])) {
            children[name] = this._children[name].map(aggregateChild);
        } else if (this._children[name]) {
            children[name] = aggregateChild(this._children[name]);
        }
    }

    for (name in children) {
        status[name] = children[name];
    }

    status._aggregates = aggregates;

    return status;
};

Status.prototype.setStatus = function (status) {
    this._data = status;
    this._aggregates = status._aggregates || {};
    delete this._data._aggregates;
};
