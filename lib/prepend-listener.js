'use strict';

module.exports = function prependListener(emitter, event, listener) {
    // Remove all listeners
    const listeners = emitter.listeners(event) || [];
    emitter.removeAllListeners(event);

    // Add our listener first
    const result = emitter.on(event, listener);

    // Add the old listeners then
    listeners.forEach((fn) => emitter.on(event, fn));

    return result;
};
