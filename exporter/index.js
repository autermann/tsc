#!/usr/bin/env node

var path = require('path');
var copy = require('./lib/copy');
var execute = require('./lib/execute');
var mongodb = 'mongodb://localhost:27017/enviroCar';
var postgres = 'postgres://postgres:postgres@localhost:5432/envirocar';


var bbox = {
  x: [
    6.098785400390625,
    6.7723846435546875
  ],
  y: [
    50.99560744780919,
    51.39363622420581
  ],
  t: [
    '2016-06-06T00:00:00.000Z',
    '2016-07-03T23:59:59.999Z'
  ]
};

copy(mongodb, postgres, {
  time: {
    $gte: new Date(bbox.t[0]),
    $lte: new Date(bbox.t[1])
  },
  geometry: {
    $geoWithin: {
      $geometry: {
        type: "Polygon",
        coordinates: [[
          [bbox.x[0], bbox.y[0]],
          [bbox.x[0], bbox.y[1]],
          [bbox.x[1], bbox.y[1]],
          [bbox.x[1], bbox.y[0]],
          [bbox.x[0], bbox.y[0]]
        ]]
      }
    }
  }
}, console.error.bind(console));
