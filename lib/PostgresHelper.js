
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
  return 'COPY ' + tableName + '(' + fields.map(function(x) {return x.name;}).join(', ') + ') FROM STDIN';
}

var COLUMN_SEPERATOR = '\t';
var RECORD_SEPERATOR = '\n';
var NULL_VALUE = '\\N';

function PostgresHelper(options) {
  options = options || {};
  this.tableName = options.tableName || 'measurements';
  this.phenomenons = options.phenomenons || [];
  this.phenomenonsAndUnits = this.phenomenons.reduce(function(array, phen) {
    array.push({ name: phen, type: 'double precision' });
    array.push({ name: phen + '_unit', type: 'char(16)' });
    return array;
  }, []);
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

  this.phenomenonsAndUnits.forEach(function(phenomenon) {
    var value = measurement.values[phenomenon.name];
    stream.write(this.columnSeperator);
    if (value === undefined || value === null) {
      stream.write(this.nullValue);
    } else {
      stream.write(value.toString());
    }
  }, this);

  stream.write(this.recordSeperator);
};

PostgresHelper.prototype.createCommand = function () {
  return createTable(this.tableName, this.fields.concat(this.phenomenonsAndUnits));
};

PostgresHelper.prototype.copyCommand = function() {
  return copyFromStdin(this.tableName, this.fields.concat(this.phenomenonsAndUnits));
};

module.exports = PostgresHelper;
