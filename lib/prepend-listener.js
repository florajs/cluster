'use strict';

module.exports = function prependListener(emitter, event, listener) {
    var listeners = emitter.listeners(event) || [];
    emitter.removeAllListeners(event);

    var result = emitter.on(event, listener);

    for (var i = 0, len = listeners.length; i < len; i++) {
        emitter.on(event, listeners[i]);
    }

    return result;
};
