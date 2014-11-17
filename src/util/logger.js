'use strict';
var _ = require('lodash');
var LOG_LEVELS = ['info', 'error', 'debug', 'warn'];

var instances = {};

module.exports = function sqlLogger(sql) {
  if (!instances[sql]) {
    instances[sql] = _(LOG_LEVELS).zipObject().mapValues(function(x, level) {
      return function(text, meta) {
        sql.insert('s3json2sql_error', {
          level: level,
          text: text,
          meta: meta ,
          loggedAt: new Date
        });
      }
    }).value();
  }
  return instances[sql];
};