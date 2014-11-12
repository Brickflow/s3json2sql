'use strict';
var _ = require('lodash');
var getType = require('./getType');
var safeName = require('./safeName');

module.exports =  function typedData(data) {
  return _.reduce(data, function(acc, v, k) {
//    acc[safeName(k) + '__' + getType(v).suffix] = v;
    acc[safeName(k)] = v;
    return acc;
  }, {});
};