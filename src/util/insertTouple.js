'use strict';
var _ = require('lodash');
var wrap = require('./wrap');

module.exports = function insertTouple(data, sqlEscape) {
  var touples = _(data).pairs().zip().value();
  touples[0] = wrap(_.map(touples[0], function (fieldName) {
    return wrap(fieldName, '`');
  }), '(');
  touples[1] = wrap(_.map(touples[1], function (value) {
    return sqlEscape(value);
  }), '(');
  return touples.join(' VALUES ');
};