'use strict';
var _ = require('lodash'),
    async = require('async');

module.exports = function getS3Objects(s3, s3conf, fileTask, cb) {
  var files = [];
  s3.listObjects({s3Params: {
    Bucket: s3conf.bucket,
    Prefix: s3conf.prefix
  }}).on('data', function(res) {
    console.log('DATA HAPPENED');
    files = files.concat(res.Contents);
  }).on('end', function() {
    console.log('TEH END', files.length);
    async.eachSeries(files, fileTask, cb);
  }).on('error', function() {
    console.log('listObjects ERRORKA', arguments);
  });
};