
var MongoClient = require('mongodb').MongoClient;

module.exports = function iterateEntities(url, collection, q, callback) {

  MongoClient.connect(url, function onConnect(err, db) {
    var cursor;

    if (err) {
      return callback(err, null);
    } else {
      console.log("Query: " + JSON.stringify(q, null, 2));
      db.collection(collection).count(q).then(function(count) {
        console.log("Iterating over " + count + " entities");
        cursor = db.collection(collection).find(q);
        cursor.next(consume);
      }).catch(function(err) {
        close(err, null);
      });
    }

    function close(err, result) {
      cursor.close(function onCursorClose() {
        db.close(false, function onDbClose() {
          callback(err, result);
        });
      });
    }

    function consume(err, result) {
      if (err || result === null) {
        close(err, result);
      } else {
        callback(null, result);
        cursor.next(consume);
      }
    }

  });
};
