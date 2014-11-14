'use strict';

var _ = require('lodash'),
    flat = require('flat');
var crypto = require('crypto');
var mysql = require('mysql');
var touple = require('../util/touple'),
    insertTouple = require('../util/insertTouple'),
    wrap = require('../util/wrap'),
    getType = require('../util/getType'),
    typedData = require('../util/typedData'),
    safeName = require('../util/safeName');

var SUPRESSED_ERRORS = [
  'ER_DUP_ENTRY',
  'ER_TOO_LONG_IDENT',
  'ER_WRONG_TABLE_NAME'
];

function hashField(data) {
  return { s3json2mysqlHash: data ?
      crypto.createHash('sha1').update(JSON.stringify(data)).digest('hex') :
      getType(null, 's3json2mysqlHash').sql
  };
}

function toSqlType(val) { return getType(val).sql; }

module.exports = function mysqlDriver(uri) {
  var sql = mysql.createConnection(uri);

  function query(parts, payload, cb) {
    sql.query(_.isArray(parts) ? parts.join(' ') + ';' : parts, payload, cb);
  }

  function addColumns(table, fields, cb) {
    query([
      'ALTER TABLE', wrap(table, '`'),
      'ADD COLUMN', touple(fields)
    ], cb);
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
    query(['CREATE TABLE', wrap(table, '`'),
      touple(_.assign(fields, hashField()), '`')], function(err, res) {
      if (err) {
        console.log('s3json2sql ERROR: CREATE TABLE', table, 'FAILED: ', err);
      }
      cb(err,res);
    });
  }

  function find(table, data, cb) {
    var q = [
      'SELECT * FROM', wrap(table, '`'), 'WHERE',
      _.times(_.pairs(data).length,
          _.partial(_.identity, '?? = ?')).join(' AND ') ];
    query(q, _(data).pairs().flatten().value(), function(err, res) {
      if (err) {
        if (err.code === 'ER_NO_SUCH_TABLE') {
          createTable(table,_.mapValues(data, toSqlType), function(err) {
            if (err) { return cb(err); }
            return find(table, data, cb);
          });
        } else {
          return cb(err);
        }
      }
      return cb(null, res);
    });

  }

  function insert(table, data, cb) {
    table = safeName(table);
    data = _.assign(typedData(flat(data)), hashField(data));
    var q = [
      'INSERT INTO', wrap(table, '`'),
      insertTouple(data, sql.escape.bind(sql))
    ];
    query(q, function(err, fields, columns) {
      if (err) {
        if (err.code === 'ER_NO_SUCH_TABLE') {
          createTable(table, _.mapValues(data, toSqlType), function(err, res) {
            query(q, cb);
          });
        } if (err.code === 'ER_BAD_FIELD_ERROR') {
          getColumns(table, function(err, fields) {
            var fieldsToCreate =
                _(data).
                    pick(_.difference( _.keys(data), _.keys(fields)) ).
                    mapValues(toSqlType).value();
            addColumns(table, fieldsToCreate, function(err, res) {
              if (err && err.code) {
                cb(err);
              } else {
                query(q, cb);
              }
            });
          });
        } else if (_.contains(SUPRESSED_ERRORS, err.code)) {
          setImmediate(cb);
        } else {
          cb(err);
        }
      } else {
        return cb.apply(null, Array.prototype.slice.call(arguments));
      }
    });
  }

  return {
    insert: insert,
    find: find,
    getColumns: getColumns,
    createTable: createTable,
    addColumns: addColumns,
    sql: sql
  };
};