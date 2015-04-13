var _ = require('lodash');
var async = require('async');
var glob = require('glob');
var parser = require('./parser');

var getListOfFiles = function(callback){
  glob('data/*.csv', function(err, files){
    callback(err,files);
  });
};

var parseFiles = function(files, callback){
  async.mapLimit(files, 5, parser.parse, function(err, results){
    callback(err, _.flatten(results));
  });
};

exports.get = function(callback){
  async.waterfall([
    getListOfFiles,
    parseFiles,
  ], callback);
};
