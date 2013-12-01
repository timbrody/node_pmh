var 
  poolModule = require('generic-pool')
  , pmh = require('./pmh')
  , mysql = require('mysql')
  , promise = require('node-promise')
  , Promise = require('node-promise').Promise
;

var celestial = function(opts) {
	this.opts = opts;
  this.harvesters = {};
	this.initialize();
};
exports = module.exports = function(opts) {
	return new celestial(opts);
};

celestial.prototype.initialize = function() {
	var _this = this;
	this.pool = poolModule.Pool({
	  //log: true,
	  name: 'mysql',
	  create: function(callback) {
		var connection = mysql.createConnection({
		  host: _this.opts.host,
		  database: _this.opts.database,
		  user: _this.opts.user,
		  password: _this.opts.password
		});
		connection.connect();

		callback(null, connection);
	  },
	  destroy: function(connection) {
		connection.end();
	  },
	  max: 10,
	  min: 2,
	  idleTimeoutMillis: 30000
	});
};

celestial.prototype.start = function(celestialid, endpoint) {
	var _this = this;
	var Harvester = new pmh.Harvester();
  this.harvesters[celestialid] = Harvester;

	Harvester
  .on('record', function(record) {
		_this.record({
      harvester: Harvester,
      celestialid: celestialid,
      record: record
    });
	})
  .on('end', function() {
    _this.harvesters[celestialid] = undefined;
  })
  .on('error', function(err) {
    console.log(err);
    _this.harvesters[celestialid] = undefined;
  })
  ;

	Harvester.request(endpoint);
};

celestial.prototype.stop = function(celestialid) {
  if (this.harvesters[celestialid] === undefined) {
    return;
  }
  this.harvesters[celestialid].stop();
  this.harvesters[celestialid] = undefined;
};

celestial.prototype.getStatus = function(celestialid) {
  if (this.harvesters[celestialid] === undefined) {
    return;
  }
  return this.harvesters[celestialid].state.status;
};

celestial.prototype.record = function(opts) {
  var _this = this
    , pool = this.pool
    , celestialid = opts.celestialid
    , record = opts.record
  ;

  pool.acquire(function(err, connection) {
    if (err) {
      _this.stop(celestialid);
      console.log(err);
    }
    else {
      var first = new Promise();
      first
      .then(function() {
        var p = new Promise();

        connection.query('BEGIN', [], function(err) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve();
          }
        });

        return p;
      })
      .then(function() {
        var p = new Promise();

        connection.query('SELECT dcid FROM dc WHERE celestialid=? AND identifier_hash=UNHEX(SHA2(?,256))', [celestialid,record.identifier], function(err, rows) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve(rows.length > 0 ? rows[0].dcid : undefined);
          }
        });

        return p;
      })
      .then(function(id) {
        var p = new Promise();

        if (id !== undefined) {
          connection.query('UPDATE dc SET status=? AND datestamp=? WHERE dcid=?', [record.status,record.datestamp,id], function(err, result) {
            if (err) {
              p.reject(err);
            }
            else {
              p.resolve(id);
            }
          });
        }
        else {
          connection.query('INSERT INTO dc (celestialid,status,datestamp,identifier_hash) VALUES(?,?,?,UNHEX(SHA2(?,256)))', [celestialid,record.status,record.datestamp,record.identifier], function(err, result) {
            if (err) {
              p.reject(err);
            }
            else {
              p.resolve(result.insertId);
            }
          });
        }

        return p;
      })
      .then(function(id) {
        var sql = [];

        var terms = pmh.dcTerms();
        for(var i = 0; i < terms.length; ++i) {
          var table = 'dc_' + terms[i];
          var term = terms[i];

          sql.push('DELETE FROM ' + table + ' WHERE dcid=' + id);

          if (record.dc[term] === undefined) {
            continue;
          }

          var values = [];
          for(var j=0; j < record.dc[term].length; ++j) {
            values.push('(' + [
                id,
                j,
                connection.escape(record.dc[term][j])
              ].join(',') + ')');
          }

          sql.push('INSERT INTO ' + table + ' (dcid,pos,' + term + ') VALUES ' + values.join(','));
        }

        var promises = [];

        while(sql.length > 0) {
          // force a closure
          (function(sql) {
            promises.push(function() {
              var p = new Promise();
              connection.query(sql, [], function(err) {
                if (0 && err) {
                  p.reject(err);
                }
                else {
                  p.resolve(id);
                }
              });
              return p;
            });
          })(sql.shift());
        }

        return promise.seq(promises);
      })

      // release the connection and report
      .then(function(id) {
        connection.query('COMMIT', [], function(err) {
          pool.release(connection);
          if (err) {
            console.log(err);
          }
          else {
            console.log(record.identifier);
          }
        });
      }, function(err) {
        connection.query('ROLLBACK', [], function() {
          console.log(err, celestialid, record.identifier);
          pool.release(connection);
        });
      });

      first.resolve();
    }
  });
};
