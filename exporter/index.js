#!/usr/bin/env node

var path = require('path');

var copy = require('./lib/copy');
var execute = require('./lib/execute');
var mongodb = 'mongodb://localhost:27017/enviroCar';
var postgres = 'postgres://postgres:postgres@localhost:5432/enviroCar';
var query = {};

copy(mongodb, postgres, query, console.error.bind(console));
