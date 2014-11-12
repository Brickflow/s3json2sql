'use strict';
var _ = require('lodash');

var s3lib = require('s3');
var parseS3Objects = require('./parseS3Objects');

var sqlDriver = require('./drivers/mysql');

_.mixin('flattenObject')

function run(conf, s3, sql) {
  parseS3Objects(s3, conf.s3, function eachLogEntryTask(str, done) {
    var json = JSON.parse(str);
    var payload = _(json).omit('text', 'meta').assign(json.meta).value();
    var tableName = str.text;
    sql.insert(json.text, payload, done);
  });
}

module.exports = function s3json2sql(conf) {
  if (conf && typeof conf.sql === 'string') {
    conf.sql = {uri: conf.sql};
  }
  conf = _.defaults(conf || {
    s3: {
      bucket: 'logs',
      prefix: '',
      filePath: /.+\.txt$/i,
      s3Options: {
        accessKeyId: '[s3 key here]',
        secretAccessKey: '[s3 secret here]'
      }
    },
    sql: {
      uri: 'mysql://...'
    }
  });

  var s3 = s3lib.createClient(_.omit(conf.s3, 'bucket', 'prefix', 'filePath'));
  var sql = sqlDriver(conf.sql.uri);
  run(conf, s3, sql);
};