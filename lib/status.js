'use strict';

const removeValue = require('./remove-value');

/**
 * Simple status tracking
 */
class Status {
    constructor() {
        this.name = null;
        this.parent = null;

        this._data = {};
        this._aggregates = {};
        this._callbacks = [];
        this._children = {};
    }

    set(name, value) {
        this._data[name] = value;
    }

    increment(name, increment, destObj, aggregates) {
        (aggregates || this._aggregates)[name] = 'increment';
        destObj = destObj || this._data;

        if (!destObj[name]) destObj[name] = 0;
        destObj[name] += increment || (typeof increment === 'undefined' ? 1 : 0);
    }

    setIncrement(name, value) {
        this._aggregates[name] = 'increment';
        this._data[name] = value;
    }

    onStatus(callback) {
        this._callbacks.push(callback);
    }

    child(name) {
        const childStatus = new Status();
        childStatus.name = name;
        childStatus.parent = this;

        this._children[name] = childStatus;

        return childStatus;
    }

    addChild(name) {
        const childStatus = new Status();
        childStatus.name = name;
        childStatus.parent = this;

        if (!Array.isArray(this._children[name])) this._children[name] = [];
        this._children[name].push(childStatus);

        return childStatus;
    }

    close() {
        // close/aggregate children first if still open:
        Object.keys(this._children).forEach((name) => {
            if (Array.isArray(this._children[name])) {
                this._children[name].forEach((child) => {
                    if (child) child.close();
                });
            } else if (this._children[name]) this._children[name].close();
        });

        if (this.parent) {
            Object.keys(this._aggregates).forEach((name) => {
                this.parent[this._aggregates[name]](name, this._data[name]);
            });

            if (Array.isArray(this.parent._children[this.name])) {
                removeValue(this.parent._children[this.name], this);
            } else {
                this.parent._children[this.name] = null;
            }
            this.parent = null;
        }

        this._callbacks = null; // avoid memleaks
    }

    getStatus() {
        const status = {};

        if (Array.isArray(this._callbacks)) {
            this._callbacks.forEach(callback => callback.call(this));
        }

        Object.assign(status, this._data);

        const aggregates = this._aggregates;

        function aggregateChild(child) {
            const childStatus = child.getStatus();
            Object.keys(childStatus._aggregates).forEach((name) => {
                child.parent[childStatus._aggregates[name]](name, childStatus[name],
                    status, aggregates);
            });
            delete childStatus._aggregates;

            return childStatus;
        }

        const children = {};
        Object.keys(this._children).forEach((name) => {
            if (Array.isArray(this._children[name])) {
                children[name] = this._children[name].map(aggregateChild);
            } else if (this._children[name]) {
                children[name] = aggregateChild(this._children[name]);
            }
        });

        Object.assign(status, children);

        status._aggregates = aggregates;

        return status;
    }

    setStatus(status) {
        this._data = status;
        this._aggregates = status._aggregates || {};
        delete this._data._aggregates;
    }
}

module.exports = Status;
