
var MongoClient = require('mongodb').MongoClient;

module.exports = function iterateEntities(url, collection, q, callback) {

  MongoClient.connect(url, function onConnect(err, db) {
    var cursor;

    if (err) {
      return callback(err, null);
    } else {
      cursor = db.collection(collection).find(q);
      cursor.next(consume);
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
