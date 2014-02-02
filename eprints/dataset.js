var
  promise = require('node-promise')
;

/* schema:
 * {
 *  "name": "celestial",
 *  "table": "celestial",
 *  "fields": {
 *    "celestialid": {
 *      "type": "int"
 *    },
 *    ...
 *  },
 *  "primary_key": ["celestialid"]
 * }
 *
 */

function Dataset(schema) {
  this.schema = schema;
}
exports.Dataset = Dataset;

Dataset.prototype.get = function(connection, ids, callback, next) {
  var _this = this;
  var objects = {};
  var keyfield = this.schema.primary_key[0];
  connection.query('SELECT * FROM ' + this.schema.table + ' WHERE ' + keyfield + ' IN (' + ids.map(function() { return '?'; }).join(','), ids, function(err, rows) {
    if (err) {
      console.log(err);
      return;
    }
    var object = {};
    for(var fieldid in _this.schema.fields) {
      switch(_this.schema.fields[fieldid]) {
        case "date":
          object[fieldid] = new Date(
        default:
      }
    }
  });
};
