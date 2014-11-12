'use strict';
var _ = require('lodash'),
    async = require('async');
var getS3Objects = require('./getS3Objects');

module.exports = function parseS3Objects(s3, s3conf, sql, rowTask, callback) {
  getS3Objects(s3, s3conf, function fileTask(file, cb) {
    var q = 'SELECT COUNT(`key`) AS cnt FROM s3json2sql WHERE `key`="' + file.Key + '"';
    sql.sql.query(q, function(err, res) {
      if (err) {
        console.log('NEMDERULTKI', err.code, err);
        return cb(err);
      } else if (res[0].cnt === 0) {
        s3.downloadBuffer({
          Bucket: s3conf.bucket,
          Key: file.Key
        }).on('error', function() {
          console.log('downloadBuffer ERRORKA', arguments)
        }).on('end', function(buffer) {
          return async.eachSeries(_.compact(buffer.toString('utf8').split('\n')), rowTask, cb);
        });
      } else {
        console.log('HAS THIS FILE PROCESSED');
        return cb();
      }
    });
  }, callback);
};