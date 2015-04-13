var _ = require('lodash');
var restify = require('restify');
var ingest = require('../ingestor/run');

var server = restify.createServer();

server.use(restify.queryParser({mapParams:false}));
server.use(restify.CORS());

var shotData = null;
var filters = {};

server.get('/filters', function(req, res, next){
  res.send(filters);
});

server.get('/shots', function(req, res, next){
  var query = _.pick(req.query, 'match', 'team', 'player');
  var shots = _.filter(shotData, query);
  response = shots.reduce(function(acc, shot){
    acc[shot.x] = acc[shot.x] || {};
    acc[shot.x][shot.y] = acc[shot.x][shot.y] || {hits:0, attempts:0};
    if (shot.hit) acc[shot.x][shot.y].hits++;
    acc[shot.x][shot.y].attempts++;
    return acc;
  }, {});  
  res.send(response);
});

ingest.get(function(err, data){
  if (err) {
    res.send(503);
    next();
  } else {
    shotData = data;
    filters = {
      players: _(shotData).pluck('player').unique().value(),
      teams: _(shotData).pluck('team').unique().value() ,
      matches: _(shotData).pluck('match').unique().value()
    };
    server.listen(8080, function() {
      console.log('%s listening at %s', server.name, server.url);
    });
  }
});  

