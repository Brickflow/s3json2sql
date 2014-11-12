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
    console.log(parts);
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
    query(['CREATE TABLE', wrap(table, '`'), touple(_.assign(fields, hashField()), '`')], cb);
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
        switch(err.code) {
          case 'ER_NO_SUCH_TABLE':
            createTable(table, _.mapValues(data, toSqlType), function(err, res) {
              query(q, cb);
            });
            break;
          case 'ER_BAD_FIELD_ERROR':
            getColumns(table, function(err, fields) {
              var missingFieldNames = _.difference(_.keys(data), _.keys(fields));
              var fieldsToCreate =
                  _(data).pick(missingFieldNames).mapValues(toSqlType).value();
              addColumns(table, fieldsToCreate, function(err, res) {
                if (err && err.code) {
                  cb(err);
                } else {
                  query(q, cb);
                }
              });
            });
            break;
          case 'ER_DUP_ENTRY':
            return cb();
          case 'ER_TOO_LONG_IDENT':
            console.log('ER_TOO_LONG_IDENT VAN');
            return cb();
          default:
            console.log('SOME KINDA ERRORKA', err.code);
            return cb(err);
        }
      } else {
        return cb.apply(null, Array.prototype.slice.call(arguments));
      }
    });
  }

  return {
    insert: insert,
    getColumns: getColumns,
    createTable: createTable,
    addColumns: addColumns,
    sql: sql
  };
};