#!/usr/bin/env node

var path = require('path');

var copy = require('./lib/copy');
var execute = require('./lib/execute');
var mongodb = 'mongodb://localhost:27017/enviroCar';
var postgres = 'postgres://postgres:postgres@localhost:5432/enviroCar';
var query = {};
/*
copy(mongodb, postgres, query, function(err) {
  if (err) {
    console.error(err);
  } else {
    var sqlScript = path.join(__dirname, 'lib', 'postprocess.sql');
    execute(postprocess, sqlScript, function(err, result) {
      if (err) {
        console.error(err);
      } else {
        console.log(result);
      }
    });
  }
});
*/


var postprocess = path.join(__dirname, 'lib', 'postprocess.sql');
    execute(postgres, postprocess, function(err, result) {
      if (err) {
        console.warn("Fehler:" + err);
      } else {
        console.log(result);
      }
    });
