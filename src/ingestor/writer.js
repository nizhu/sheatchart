var _ = require('lodash');
var async = require('async');
var pg = require('pg');

var insertTeams = function(pgClient, data, callback){
  var teams = _(data)
    .pluck('team')
    .unique()
    .value();
  async.map(teams, function(team, cb){
    pgClient.query('INSERT INTO teams (name) VALUES ($1)', [team], cb);
  }, callback); 
};

var getHomeTeamName = function(matchTeamStr){
  return matchTeamStr.substring(0,3);
};

var getAwayTeamName = function(matchTeamStr){
  return matchTeamStr.substring(3,6);
};

var formatDate = function(matchDateStr){
  return matchDateStr.substring(0,4) + '-' + 
   matchDateStr.substring(4,6) + '-' + 
   matchDateStr.substring(6,8);
};

var fetchTeams = function(pgClient, callback){
  pgClient.query('SELECT id, name from teams', function(err, results){
    if (err) return callback(err);
    callback(null, results.rows.reduce(function(acc, row){
      acc[row.name] = row.id;
      return acc;
    }, {}));
  });
};

var fetchPlayers = function(pgClient, callback){
  pgClient.query('SELECT id, name from players', function(err, results){
    if (err) return callback(err);
    callback(null, results.rows.reduce(function(acc, row){
      acc[row.name] = row.id;
      return acc;
    }, {}));
 });  
};

var fetchMatches = function(pgClient, callback){
  pgClient.query('SELECT m.id as id, ht.name as home_team, at.name as away_team, m.game_date as date FROM matches m JOIN teams ht ON m.home_team=ht.id JOIN teams at ON m.away_team=at.id', function(err, results){ 
    if (err) return callback(err);
    console.log(results.rows);
    callback(null, results.rows.reduce(function(acc, row){
      acc[row.name] = row.id;
      return acc;
    }, {}));
  });  
};

var insertMatches = function(pgClient, data, callback){
  fetchTeams(pgClient, function(err, teamIds){
    if (err) return callback(err);
    var matches = _(data)
      .pluck('match')
      .unique()
      .map(function(match){
        var parts = match.split('.');
        console.log(parts[0]);
        return [
          teamIds[getHomeTeamName(parts[1])],
          teamIds[getAwayTeamName(parts[1])],
          formatDate(parts[0])
        ];
      })
      .value()
    async.map(matches, function(match, cb){
      pgClient.query('INSERT INTO matches (home_team, away_team, game_date) VALUES ($1, $2, $3)', match, cb);
    }, callback);
  });
};

var insertPlayers = function(pgClient, data, callback){
  var players = _(data)
    .pluck('player')
    .unique()
    .value();
  async.map(players, function(player, cb){
    pgClient.query('INSERT INTO players (name) VALUES ($1)', [player], cb);
  }, callback); 
};

var insertShots = function(pgClient, data, callback){
  async.series([
    fetchTeams.bind(null, pgClient),
    fetchPlayers.bind(null, pgClient),
    fetchMatches.bind(null, pgClient)
  ], function(err, teams, players, matches){
    if (err) return callback(err);
    console.log(teams, players, matches);
    callback();
  });
};

exports.ingest = function(data, callback){
  pg.connect('postgres://postgres:postgrespassword@localhost/shots', function(err, client, done){
    if (err) return callback(err);
    async.series([
      insertTeams.bind(null, client, data),
      insertMatches.bind(null, client, data),
      insertPlayers.bind(null, client, data),
      insertShots.bind(null, client, data)
    ], function(err){
      done(client);
      callback(err);
    });
  });
};

