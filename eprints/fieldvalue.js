

function FieldValue(id, schema) {
  this.id = id;
  this.schema = schema;
}
exports.FieldValue = FieldValue;

FieldValue.prototype.getValue = function() {
  return this.value;
}

FieldValue.prototype.setValue = function(value) {
  this.value = value;
}

FieldValue.prototype.setValueFromRow = function(row) {
  this.value = row[this.id];
};
