'use strict';
var _ = require('lodash');
var s3lib = require('s3');
var orm2 = require('orm');

var parseS3Objects = require('./parseS3Objects');

function run(conf, s3, db) {
  parseS3Objects(s3, conf.s3, function(row, done) {
    var json = JSON.parse(row);
    console.log('DAT JSON');
    console.dir(json);
    process.nextTick(done);
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
    sql: {uri: 'mysql://...'},
    tmpDir: __dirname + '/tmp'
  });
  var s3 = s3lib.createClient(_.omit(conf.s3, 'bucket', 'prefix', 'filePath'));
  var sql = orm2.connect(conf.sql.uri, function(err, db) {
    if (err) {
      throw err;
    }
    run(conf, s3, db);
  });
};