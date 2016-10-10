
var MongoClient = require('mongodb').MongoClient;
var ProgressBar = require('progress');

module.exports = function iterateEntities(url, collection, q, callback) {

  MongoClient.connect(url, function onConnect(err, db) {
    var cursor;
    if (err) {
      return callback(err, null);
    } else {
      console.log("Query: " + JSON.stringify(q, null, 2));
      db.collection(collection).count(q).then(function(count) {

        var bar = new ProgressBar('[:bar] :current/:total ETA: :etas', {
          total: count,
          complete: '#',
          incomplete: '-',
          renderThrottle: 1000
        });
        cursor = db.collection(collection).find(q);
        cursor.next(consume);
        function consume(err, result) {
          if (err || result === null) {
            close(err, result);
          } else {
            bar.tick();
            callback(null, result);
            cursor.next(consume);
          }
        }

      }).catch(function(err) {
        close(err, null);
      });
    }

    function close(err, result) {
      function dbClose() {
        if (db) {
          db.close(false, function onDbClose() {
            callback(err, result);
          });
        } else {
          callback(err, result);
        }
      }
      if (cursor) {
        cursor.close(function onCursorClose() {
          dbClose();
        });
      } else {
        dbClose();
      }

    }
  });
};
