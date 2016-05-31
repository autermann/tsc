
var pg = require('pg');
var minify = require('pg-minify');
var fs = require('fs');

module.exports = function execute(postgresURL, file, callback) {
  var client = new pg.Client(postgresURL);

  fs.readFile(file, 'utf8', function(err, data) {
    if (err) {
      callback(err);
    } else {
      client.connect(function(err) {
        if (err) {
          callback(err);
        } else {
          var query = minify(data);
          console.log(query);
          client.query(query, function(err, result) {
            client.end();
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }
          });
        }
      });
    }
  });
};