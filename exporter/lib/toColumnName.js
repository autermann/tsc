
var regex = /[^a-z0-9]/g;

module.exports = function toColumnName(name) {
  return name.toLowerCase().replace(regex, '_');
};
