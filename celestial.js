var 
  poolModule = require('generic-pool')
  , pmh = require('./pmh')
  , mysql = require('mysql')
  , promise = require('node-promise')
  , Promise = require('node-promise').Promise
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
;

var celestial = function(opts) {
  EventEmitter.call(this);
	this.opts = opts;
  this.harvesters = {};
	this.initialize();
};
util.inherits(celestial, EventEmitter);
exports = module.exports = function(opts) {
	return new celestial(opts);
};

var written = {};
celestial.prototype.initialize = function() {
	var _this = this;
	this.pool = poolModule.Pool({
	  //log: true,
	  name: 'mysql',
	  create: function(callback) {
		var connection = mysql.createConnection({
		  host: _this.opts.database.host,
		  database: _this.opts.database.database,
		  user: _this.opts.database.user,
		  password: _this.opts.database.password
		});
		connection.connect();

		callback(null, connection);
	  },
	  destroy: function(connection) {
		connection.end();
	  },
	  max: 10,
	  min: 2,
	  idleTimeoutMillis: 30000,
    priorityRange: 2
	});
};

celestial.prototype.log = function(err) {
  console.log(err);
};

celestial.prototype.start = function(repo) {
	var _this = this
    , celestialid = repo.celestialid
    , oai_dc
  ;
  for(var i=0; i<repo.formats.length; ++i) {
    if (repo.formats[i].prefix === 'oai_dc') {
      oai_dc = repo.formats[i];
    }
  }

	var harvester = new pmh.Harvester({
    headers: this.opts.headers
  });
  this.harvesters[celestialid] = harvester;

  var tokens = [];

	harvester
  .on('record', function(record) {
		_this.record({
      harvester: harvester,
      celestialid: celestialid,
      record: record
    });
	})
  .on('resume', function(opts, next) {
    var seen = false;
    for(var i=0; i < tokens; ++i) {
      if (opts.resumptionToken === tokens[i]) {
        seen = true;
        break;
      }
    }
    if (seen) {
      harvester.emit('error', 'Resumption token repeated: ' + opts.resumptionToken);
    }
    else {
      while(tokens.length >= 20) {
        tokens.shift();
      }
      tokens.push(opts.resumptionToken);
      // wait until all database writes are complete before continuing
      _this.pool.acquire(function(err, connection) {
        _this.pool.release(connection);
        next();
      }, 1);
    }
  })
  .on('end', function() {
    delete _this.harvesters[celestialid];
  })
  .on('error', function(err) {
    delete _this.harvesters[celestialid];
  })
  ;

  // wait until all database writes are complete before continuing
  _this.pool.acquire(function(err, connection) {
    _this.pool.release(connection);
    var opts = {};
    if (oai_dc.token) {
      opts.resumptionToken = oai_dc.token;
    }
    else if (oai_dc.harvest && oai_dc.harvest.length) {
      var from = new Date(oai_dc.harvest);
      opts.from = from.toISOString().substring(0,10);
    }
    harvester.request(repo.url, opts);
  }, 1);

  return harvester;
};

celestial.prototype.stop = function(celestialid) {
  if (this.harvesters[celestialid] === undefined) {
    return;
  }
  this.harvesters[celestialid].stop();
  delete this.harvesters[celestialid];
};

celestial.prototype.currentState = function() {
  var result = {};
  for(var celestialid in this.harvesters) {
    result[celestialid] = this.harvesters[celestialid].state;
  }
  return result;
};

celestial.prototype.record = function(opts) {
  var _this = this
    , pool = this.pool
    , celestialid = opts.celestialid
    , record = opts.record
    , harvester = opts.harvester
  ;

  pool.acquire(function(err, connection) {
    if (err) {
      _this.stop(celestialid);
      _this.log({
        celestialid: celestialid,
        harvester: harvester,
        error: err
      });
    }
    else {
      var begin = function(ok) {
        var p = new Promise();

        connection.query('BEGIN', [], function(err) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve(ok);
          }
        });

        return p;
      };
      var commit = function(ok) {
        var p = new Promise();

        connection.query('COMMIT', [], function(err) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve(ok);
          }
        });

        return p;
      };

      var first = new Promise();
      first
      .then(begin)
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
        return commit(id);
      })
      .then(function(id) {
        // commit the foreign key or we get deadlocks on the main table
        var sql = [];

        var terms = pmh.dcTerms();
        for(var i = 0; i < terms.length; ++i) {
          var table = 'dc_' + terms[i];
          var term = terms[i];

          sql.push('BEGIN');
          sql.push('DELETE FROM ' + table + ' WHERE dcid=' + id);

          if (record.dc[term] === undefined) {
            sql.push('COMMIT');
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
          sql.push('COMMIT');
        }

        var promises = [];

        while(sql.length > 0) {
          // force a closure
          (function(sql) {
            promises.push(function() {
              var p = new Promise();
              connection.query(sql, [], function(err) {
                if (err) {
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
        pool.release(connection);
        //console.log(record.identifier);
      }, function(err) {
        // Deadlock
        if (err.errno == 1213) {
          // try again (that's what the MySQL manual says)
          pool.release(connection);
          _this.record(opts);
        }
        else {
          connection.query('ROLLBACK', [], function() {
            _this.log({
              celestialid: celestialid,
              harvester: harvester,
              error: err
            });
            pool.release(connection);
          });
        }
      });

      first.resolve();
    }
  });
};
