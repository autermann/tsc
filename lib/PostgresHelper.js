
function createFieldDefinition(field) {
  var sql = field.name + ' ' + field.type;
  if (field.constraints) {
    field += ' ' + field.constraints;
  }
  return sql;
}

function createTable(tableName, fields) {
  fields = fields.map(createFieldDefinition).join(', ');
  return 'CREATE TABLE ' + tableName + ' (' + fields + ')';
}

function copyFromStdin(tableName, fields) {
  return 'COPY ' + tableName + '(' + fields.join(', ') + ') FROM STDIN';
}

var COLUMN_SEPERATOR = '\t';
var RECORD_SEPERATOR = '\n';
var NULL_VALUE = '\\N';

function PostgresHelper(options) {
  options = options || {};
  this.tableName = options.tableName || 'measurements';
  this.phenomenons = options.phenomenons || [];
  this.columnSeperator = options.columnSeperator || COLUMN_SEPERATOR;
  this.recordSeperator = options.recordSeperator || RECORD_SEPERATOR;
  this.nullValue = options.nullValue || NULL_VALUE;
  this.fields = [
    { name: 'id', type: 'char(24)', constraints: 'primary key' },
    { name: 'geom', type: 'geometry(Point, 4326)', constraints: 'not null' },
    { name: 'time', type: 'timestamp', constraints: 'not null' },
    { name: 'sensor', type: 'char(24)', constraints: 'not null' },
    { name: 'track', type: 'char(24)', constraints: 'not null' }
  ];
}

PostgresHelper.prototype.dropCommand = function () {
  return 'DROP TABLE IF EXISTS ' + this.tableName;
};

PostgresHelper.prototype.writeToStream = function(stream, measurement) {
  stream.write(measurement.id);
  stream.write(this.columnSeperator);
  stream.write(measurement.geom);
  stream.write(this.columnSeperator);
  stream.write(measurement.time);
  stream.write(this.columnSeperator);
  stream.write(measurement.sensor);
  stream.write(this.columnSeperator);
  stream.write(measurement.track);

  this.phenomenons.forEach(function(phenomenon) {
    var value = measurement.values[phenomenon];
    stream.write(this.columnSeperator);
    if (value !== undefined && value !== null) {
      stream.write(value.toString());
    } else {
      stream.write(this.nullValue);
    }
  }, this);

  stream.write(this.recordSeperator);
};

PostgresHelper.prototype.createCommand = function () {
  return createTable(this.tableName,
    this.phenomenons.reduce(function(fields, phen) {
      return fields.concat({ name: phen, type: 'double precision' },
                           { name: phen + '_unit', type: 'char(16)' });
    }, this.fields));
};

PostgresHelper.prototype.copyCommand = function() {
  var fields = this.fields.map(function(x){return x.name;})
    .concat(this.phenomenons.reduce(function(array, phenomenon) {
      return array.concat(phenomenon, phenomenon + '_unit');
    }, []));
  return copyFromStdin(this.tableName, fields.concat(this.phenomenons));
};

module.exports = PostgresHelper;
