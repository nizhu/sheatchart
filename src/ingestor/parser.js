var fs = require('fs');
var path = require('path');
var split = require('split');

exports.parse = function(filePath, callback){
  data = []
  matchName = path.basename(filePath, '.csv');
  fs.createReadStream(filePath)
    .pipe(split())
    .on('data', function(line){
      if (!line) return
      var parts = line.split(',');
      parts[2] = (parts[2] === '1')
      data.push({
        match: matchName,
        team: parts[0],
        player: parts[1],
        hit: parts[2],
        x: parts[3],
        y: parts[4]
      });
    })
    .on('error', callback)
    .on('close', function(){
      console.log('Processed ' + filePath);
      callback(null, data);
    });
};

