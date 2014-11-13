'use strict';
var _ = require('lodash'),
    async = require('async');
var getS3Objects = require('./getS3Objects');

module.exports = function parseS3Objects(s3, s3conf, sql, rowTask, callback) {
  getS3Objects(s3, s3conf, function fileTask(file, cb) {
    sql.find('s3json2sql', _.pick(file,
        'Key', 'LastModified'), function(err, res) {
      if (err) {
        console.log('NEMDERULTKI', err.code, err);
        return cb(err);
      } else if (res && res.length === 0) {
        s3.downloadBuffer({
          Bucket: s3conf.bucket,
          Key: file.Key
        }).on('error', function() {
          console.log('s3json2sql: s3.downloadBuffer error', arguments)
        }).on('end', function(buffer) {
          return async.eachSeries(
              _.compact(buffer.toString('utf8').split('\n')),
              rowTask,
              function(err, res) {
                if (err) {
                  return cb(err);
                }
                sql.insert('s3json2sql', _.pick(file, 'Key', 'LastModified'), cb);
              });
        });
      } else {
        console.log('s3json2sql: This file has already been processed.');
        return cb();
      }
    });
  }, callback);
};