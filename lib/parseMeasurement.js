

var toColumnName = require('./toColumnName');
var geojson2ewkt = require('./geojson2ewkt');

module.exports = function parseMeasurement(m) {
	if (!m.track || !m.phenomenons || !m.sensor || !m.time || !m.geometry) {
		return null;
	}
  return {
    id: m._id.toHexString(),
    sensor: m.sensor._id.toHexString(),
  	track: m.track.oid.toHexString(),
    time: m.time.toISOString(),
    geom: geojson2ewkt(m.geometry),
    values: m.phenomenons.reduce(function(o, x) {
      var p = toColumnName(x.phen._id);
  		o[p] = x.value;
  		o[p + '_unit'] = x.phen.unit;
  		return o;
  	}, {})
  };
};
