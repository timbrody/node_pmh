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
  this.state = {};
};

celestial.prototype.log = function(err) {
  console.log(err);
};

celestial.prototype.start = function(repo) {
	var _this = this
    , celestialid = repo.celestialid
    , oai_dc
    , rconnection
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
  this.state[celestialid] = {
    celestialid: celestialid,
    status: 'waiting',
    start: new Date(),
    records: 0,
    requests: 0
  };

  var tokens = [];

  var record_buffer = [];

	harvester
  .on('record', function(record) {
    record_buffer.push({
      harvester: harvester,
      celestialid: celestialid,
      record: record
    });
    if (record_buffer.length > 50) {
      _this.write(record_buffer);
      record_buffer = [];
    }
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
      // throttle requests by putting them in the same database-wait queue
      _this.pool.release(rconnection);
      setTimeout(function() {
        _this.pool.acquire(function(err, conn) {
          rconnection = conn;
          next();
        }, 1);
      // only resume after 5 seconds, to avoid rushing the remote server
      }, 5000);
    }
  })
  .on('end', function() {
    _this.write(record_buffer);
    record_buffer = [];
    _this.pool.release(rconnection);
    delete _this.harvesters[celestialid];
  })
  .on('error', function(err) {
    _this.pool.release(rconnection);
    delete _this.harvesters[celestialid];
  })
  // state changes
  .on('request', function(url) {
    _this.state[celestialid].request = url.format();
    _this.state[celestialid].requests++;
    _this.state[celestialid].status = 'requesting';
    _this.emit('state', _this.state[celestialid]);
  })
  .on('response', function(req, res) {
    _this.state[celestialid].statusCode = res.statusCode;
    _this.state[celestialid].status = 'parsing';
    _this.emit('state', _this.state[celestialid]);
  })
  .on('response_end', function() {
    delete _this.state[celestialid].statusCode;
    _this.state[celestialid].status = 'waiting';
    _this.emit('state', _this.state[celestialid]);
  })
  .on('redirect', function(url) {
    _this.state[celestialid].request = url;
    _this.state[celestialid].status = 'redirecting';
    _this.emit('state', _this.state[celestialid]);
  })
  .on('record', function() {
    _this.state[celestialid].records++;
    _this.emit('state', _this.state[celestialid]);
  })
  .on('error', function(err) {
    _this.state[celestialid].status = 'error';
    _this.state[celestialid].statusCode = err;
    setTimeout(function() {
      if (_this.state[celestialid].status === 'error') {
        delete _this.state[celestialid];
      }
    }, 10000);
    _this.emit('state', _this.state[celestialid]);
  })
  .on('end', function() {
    _this.state[celestialid].status = 'finished';
    _this.emit('state', _this.state[celestialid]);
    delete _this.state[celestialid];
  })
  ;

  // throttle requests by putting them in the same database-wait queue
  setTimeout(function() {
    _this.pool.acquire(function(err, conn) {
      rconnection = conn;
      var opts = {};
      if (oai_dc.token) {
        opts.resumptionToken = oai_dc.token;
      }
      else if (oai_dc.harvest && oai_dc.harvest.length) {
        var from = new Date(oai_dc.harvest);
        opts.from = from.toISOString().substring(0,10);
      }
      // catch e.g. bad URLs
      try {
        harvester.request(repo.url, opts);
      } catch (err) {
        harvester.emit('error', err);
      }
    }, 1)
  // allow database writes to get ahead in the pool
  }, 3000);

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
  return this.state;
  var result = {};
  for(var celestialid in this.harvesters) {
    result[celestialid] = this.harvesters[celestialid].state;
  }
  return result;
};

// output YYYY-MM-DDThh:mm:ss
function sqlDateTime(dt) {
  return new Date(dt).toISOString().substring(0,19);
}

/* Retrieve and update the record header (or create it if missing).
 * @param conn SQL connection
 * @param entry Record entry to retrieve/update
 */
celestial.prototype.writeHeader = function(conn, entry) {
  var p = new Promise();

  var celestialid = entry.celestialid;
  var record = entry.record;

  var accession = new Date(record.datestamp);
  if (accession.getFullYear() < 1990 || accession.getTime() > new Date().getTime()) {
    accession = new Date();
  }
  conn.query('INSERT INTO dc (celestialid,status,datestamp,accession,identifier_hash) VALUES(?,?,?,?,UNHEX(SHA2(?,256)))', [celestialid,record.status,sqlDateTime(record.datestamp),sqlDateTime(accession),record.identifier], function(err, result) {
    if (err && err.errno !== 1062) {
      p.reject(err);
    }
    else if (err) {
      conn.query('SELECT dcid FROM dc WHERE celestialid=? AND identifier_hash=UNHEX(SHA2(?,256))', [celestialid,record.identifier], function(err, rows) {
        if (err) {
          p.reject(err);
        }
        else {
          var dcid = rows[0].dcid;
          entry.dcid = dcid;
          conn.query('UPDATE dc SET status=?, datestamp=? WHERE dcid='+dcid, [record.status,sqlDateTime(record.datestamp)]);
          p.resolve();
        }
      });
    }
    else {
      var dcid = result.insertId;
      entry.dcid = dcid;
      conn.query('INSERT INTO dc_header (dcid,pos,header_identifier) VALUES (' + dcid + ',0,?)', [record.identifier]);
      p.resolve();
    }
  });

  return p;
};

celestial.prototype.write = function(entries) {
  var _this = this
    , pool = this.pool
  ;

  var celestialids = {};
  for(var i=0; i<entries.length; ++i) {
    celestialids[entries[i].celestialid] = true;
  }

  var terms = pmh.dcTerms();

  pool.acquire(function(err, connection) {
    if (err) {
      for(var celestialid in celestialids) {
        _this.stop(celestialid);
      }
      _this.log({
        error: err
      });
      return;
    }

    var first = new Promise();
    first
    .then(function() {
      return promise.all(entries.map(function(entry) {
        return _this.writeHeader(connection, entry);
      }));
    })
    .then(function() {
      return promise.all(terms.map(function(term) {
        var p = new Promise();

        var dcids = entries.map(function(entry) { return entry.dcid; });
        connection.query('DELETE FROM dc_' + term + ' WHERE dcid IN (' + dcids.join(',') + ')', [], function(err) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve();
          }
        });

        return p;
      }));
    })
    .then(function() {
      return promise.all(terms.map(function(term) {
        var p = new Promise();

        var subs = [];
        var values = [];
        for(var j=0; j<entries.length; ++j) {
          var entry = entries[j];
          var record = entry.record;
          if (record.dc[term] === undefined)
            continue;

          for(var k=0; k<record.dc[term].length; ++k) {
            subs.push('(' + [
              entry.dcid,
              k,
              '?'
            ].join(',') + ')');
            values.push(record.dc[term][k]);
          }
        }

        // nothing to do
        if (values.length === 0)
          return;

        connection.query('INSERT INTO dc_' + term + ' (dcid,pos,' + term + ') VALUES ' + subs.join(','), values, function(err) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve();
          }
        });

        return p;
      }));
    })
    .then(function() {
      _this.pool.release(connection);
      0 && entries.map(function(entry) {
        console.log(entry.celestialid, entry.dcid, entry.record.identifier);
      });
    }, function(err) {
      for(var celestialid in celestialids) {
        _this.stop(celestialid);
      }
      _this.log({
        error: err
      });
      _this.pool.release(connection);
    })
    ;

    first.resolve();
  }, 0);
};
