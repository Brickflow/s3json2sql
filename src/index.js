'use strict';
var _ = require('lodash');

var s3lib = require('s3');
var parseS3Objects = require('./parseS3Objects');

var sqlDriver = require('./drivers/mysql');

function run(conf, s3, sql) {
  parseS3Objects(s3, conf.s3, sql, function eachLogEntryTask(row, done) {
    var json = JSON.parse(row);
    var payload = _(json).
        omit(conf.s3.tableField, conf.s3.payloadField).
        assign(json[conf.s3.payloadField]).value();
    if (json[conf.s3.tableField]) {
      sql.insert(json[conf.s3.tableField], payload, done);
    } else {
      console.log('UNTITLED FOSKAZAL', conf.s3.tableField, json);
      process.nextTick(done);
    }
  }, function finalResult(err, res) {
    console.log('DAT FINAL RESULT', err, res);
  });
}

module.exports = function s3json2sql(conf) {
  if (conf && typeof conf.sql === 'string') {
    conf.sql = {uri: conf.sql};
  }
  conf = _.defaults(conf || {
    s3: {
      s3Options: {
        accessKeyId: '[s3 key here]',
        secretAccessKey: '[s3 secret here]'
      }
    },
    sql: {
      uri: 'mysql://...'
    }
  });
  _.defaults(conf.s3, {
    bucket: 'logs',
    prefix: '',
    tableField: 'text',
    payloadField: 'meta',
    filePath: /.+\.txt$/i
  });

  var s3 = s3lib.createClient(_.omit(conf.s3, 'bucket', 'prefix', 'filePath'));
  var sql = sqlDriver(conf.sql.uri);
  run(conf, s3, sql);
};