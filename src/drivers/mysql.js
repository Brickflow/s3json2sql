'use strict';

var _ = require('lodash'),
    flat = require('flat');
var mysql = require('mysql');
var touple = require('../util/touple'),
    insertTouple = require('../util/insertTouple'),
    wrap = require('../util/wrap'),
    getType = require('../util/getType'),
    typedData = require('../util/typedData'),
    safeName = require('../util/safeName');
function toSqlType(val) { return getType(val).sql; }

module.exports = function mysqlDriver(uri) {
  var sql = mysql.createConnection(uri);

  function query(parts, payload, cb) {
    console.log(parts);
    sql.query(_.isArray(parts) ? parts.join(' ') + ';' : parts, payload, cb);
  }

  function addColumns(table, fields, cb) {
    query(['ALTER TABLE', wrap(table, '`'), 'ADD COLUMN', touple(fields)], cb);
  }

  function getColumns(table, cb) {
    query(['SHOW COLUMNS FROM', wrap(table, '`')], function(err, cols) {
      cb(err, err ? null : _(cols).map(function(col) {
        return [ col.Field, col.Type ];
      }).zipObject().value());
    });
  }

  function createTable(table, fields, cb) {
    table = safeName(table);
    query(['CREATE TABLE', wrap(table, '`'), touple(fields, '`')], cb);
  }

  function insert(table, data, cb) {
    table = safeName(table);
    data = typedData(flat(data));
    var q = [
      'INSERT INTO', wrap(table, '`'),
      insertTouple(data, sql.escape.bind(sql))
    ];
    query(q , function(err, fields, columns) {
      if (err) {
        if (err.code === 'ER_NO_SUCH_TABLE') {
          createTable(table, _.mapValues(data,toSqlType), function(err, res) {
            query(q, cb);
          });
        } else if (err.code === 'ER_BAD_FIELD_ERROR') {
          getColumns(table, function(err, fields) {
//            var missingFieldNames = _(data).keys().difference(_.keys(fields)).value();
            var missingFieldNames = _.difference(_.keys(data), _.keys(fields));
            var fieldsToCreate =
                _(data).pick(missingFieldNames).
                    mapValues(safeName).
                    mapValues(toSqlType).
                    value();
            console.log('FIELDS TO CREATE', fieldsToCreate, 'XISTS', fields);
            addColumns(table, fieldsToCreate, function(err, res) {
              console.log('ADDED THOSE COLUMNSES', arguments);
              query(q, cb);
            });
          });
        } else {
          return cb(err);
        }
      }

      cb.apply(null, Array.prototype.slice.call(arguments));
    });
  }



  return {
    insert: insert,
    getColumns: getColumns,
    createTable: createTable,
    addColumns: addColumns
    // sql: sql
  };
};