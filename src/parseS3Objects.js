'use strict';
var _ = require('lodash'),
    async = require('async');
var getS3Objects = require('./getS3Objects');
var loggerFactory = require('./util/logger');

module.exports = function parseS3Objects(s3, s3conf, sql, rowTask, callback) {
  var logger = loggerFactory(sql);
  getS3Objects(s3, s3conf, function fileTask(file, cb) {
    logger.info('s3json2sql.status', { file: file.Key });
    sql.find('s3json2sql', _.pick(file,
        ['Key', 'LastModified']), function(err, res) {
      if (err) {
        logger.error('s3json2sql.status', {
          message: 'Error while checking if file has already been processed',
          err: err.toString(),
          stack: err.stack
        });
        return cb(err);
      } else if (res && res.length === 0) {
        s3.downloadBuffer({
          Bucket: s3conf.bucket,
          Key: file.Key
        }).on('error', function(err) {
          logger.error('s3json2sql.status', {
            err: err,
            stack: err.stack
          });
        }).on('end', function(buffer) {
          return async.eachSeries(
              _.compact(buffer.toString('utf8').split('\n')),
              rowTask,
              function(err) {
                if (err) {
                  logger.error('s3json2sql.status', {
                    file: file.Key,
                    err: err.toString(),
                    stack: err.stack
                  });
                  return cb(err);
                }
                logger.info('s3json2sql.status', {
                  file: file.Key
                });
                sql.insert('s3json2sql',
                    _.pick(file, 'Key', 'LastModified'), cb);
              });
        });
      } else {
        logger.warn('s3json2sql.status', {
          file: file.Key,
          message: 'File has already been processed'
        });
//        return setImmediate(cb);
        return process.nextTick(cb);
      }
    });
  }, callback);
};