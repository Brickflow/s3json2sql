'use strict';
var _ = require('lodash'),
    async = require('async');

module.exports = function parseS3Objects(s3, s3conf, task, callback) {
  s3.listObjects({s3Params: {
    Bucket: s3conf.bucket,
    Prefix: s3conf.prefix
  }}).on('data', function(res) {
    async.eachSeries(_(res.Contents).filter(function(f) {
      return !!f.Key.match(s3conf.filePath);
    }).pluck('Key').value(), function(key, cb) {
      s3.downloadBuffer({Bucket: s3conf.bucket, Key: key
      }).on('error', function() {
        console.log('downloader error', arguments);
      }).on('progress', function() {
      }).on('end', function(buffer) {
        async.eachSeries(_.compact(buffer.toString('utf8').split('\n')), task, cb);
      });
    }, callback);
  }).on('progress', function() {
    console.log('progress');
  }).on('end', function() {
    console.log('end');
  }).on('error', function(err) {
    callback(err);
  });
};
