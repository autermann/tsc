
var MongoClient = require('mongodb').MongoClient;
var toColumnName = require('./toColumnName');

module.exports = function getPhenomenons(url, callback) {
  MongoClient.connect(url, function onConnect(err, db) {
    if (err) {
      callback(err, null);
    } else {
      var cursor = db.collection('measurements').aggregate([
        { $project: { 'phenomenons': 1 } },
        { $unwind: '$phenomenons' },
        { $group: {
          _id: '1',
          phenomenons: { $addToSet: '$phenomenons.phen._id' }
        }}
      ]);
      cursor.next(function onNext(err, result) {
        cursor.close(function onCursorClose() {
          db.close(false, function onDbClose() {
            callback(err, result ? result.phenomenons.map(toColumnName).sort() : null);
          });
        });
      });
    }
  });
};
