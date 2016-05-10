var pg = require('pg');
var MongoClient = require('mongodb').MongoClient;
var copyFrom = require('pg-copy-streams').from;
var iterateEntities = require('./iterateEntities');
var parseMeasurement = require('./parseMeasurement');
var PostgresHelper = require('./PostgresHelper');
var getPhenomenons = require('./getPhenomenons');

module.exports = function copy(mongoURL, postgresURL, query, callback) {

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


      client.query(helper.dropCommand(), function(err, result) {

        if (err) {
          client.end();
          callback(err);
          return;
        }

        client.query(helper.createCommand(), function(err, result) {

          if (err) {
            client.end();
            callback(err);
            return;
          }


          var stream = client.query(copyFrom(helper.copyCommand()));
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
                console.log('Copying measurement ' + i);
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
