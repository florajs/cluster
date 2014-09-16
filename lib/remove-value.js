'use strict';

module.exports = function removeValue(array, value) {
    var index = array.indexOf(value);
    if (index !== -1) {
        return array.splice(index, 1)[0];
    }
    return null;
};
