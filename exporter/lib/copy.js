var pg = require('pg');
var copyFrom = require('pg-copy-streams').from;
var iterateEntities = require('./iterateEntities');
var parseMeasurement = require('./parseMeasurement');
var PostgresHelper = require('./PostgresHelper');
var getPhenomenons = require('./getPhenomenons');

module.exports = function copy(mongoURL, postgresURL, query, callback) {
  console.log("Getting phenomenons...");
  getPhenomenons(mongoURL, function(err, result) {

    if (err) {
      callback(err);
      return;
    }

    var helper = new PostgresHelper({
      tableName: 'measurements',
      phenomenons: result
    });

    var client = new pg.Client(postgresURL);

    client.connect(function(err) {

      if (err) {
        callback(err);
        return;
      }
      var cmd = helper.dropCommand();
      console.log('Command', cmd);

      client.query(cmd, function(err, result) {

        if (err) {
          client.end();
          callback(err);
          return;
        }
        var cmd = helper.createCommand();
        console.log('Command', cmd);
        client.query(cmd, function(err, result) {

          if (err) {
            client.end();
            callback(err);
            return;
          }

          var cmd = helper.copyCommand();
          console.log('Command', cmd);
          var stream = client.query(copyFrom(cmd));
          var i = 0;
          iterateEntities(mongoURL, 'measurements', query, function(err, m) {
            ++i;
            if (err) {
              stream.end();
              client.end();
              callback(err);
              return;
            }
            if (m !== null) {
              m = parseMeasurement(m);
              if (m !== null)  {
                if ((i % 1000) === 0) {
                  console.log('Copying measurement ' + i);
                }
                helper.writeToStream(stream, m);
              }
            } else {
              stream.end();
              client.end();
              callback();
            }
          });
        });
      });
    });
  });
};
