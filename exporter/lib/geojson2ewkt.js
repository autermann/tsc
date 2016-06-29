
var converters = {
  'Point': function(g) {
    var c = g.coordinates;
    return 'SRID=4326;POINT(' + c[0]+ ' ' + c[1] + ')';
  }
};

module.exports = function geojson2ewkt(g) {
  if (converters[g.type]) {
    return converters[g.type](g);
  }
  throw new Error('Unsupported type: ' + g.type);
};
