'use strict';
var _ = require('lodash');
var wrap = require('./wrap');

function formatFields(fields, wrapper) {
  return _(fields).map(function(type, fieldName) {
    return (typeof wrapper === 'function' ?
                wrapper(fieldName) :
                wrap(fieldName, wrapper || '')) + ' ' + type;
  }).join(', ');
}

module.exports = function touple(fields, wrapper) {
  return wrap(formatFields(fields, wrapper || ''), '(');
};