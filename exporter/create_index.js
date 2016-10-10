db.measurements.createIndex({ geometry: "2dsphere" });
db.measurements.createIndex({ time: 1 });
db.measurements.createIndex({ "phenomenons.phen._id": 1 });