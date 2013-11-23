var pmh = require('./pmh')
  , poolModule = require('generic-pool')
  , mysql = require('mysql')
  , config = require('./config.json')
  , Promise = require('node-promise').Promise
;

var endpoint = process.argv[2];

console.log('Harvesting from ' + endpoint);

var repositories = {};

repositories[endpoint] = {
};

var pool = poolModule.Pool({
  //log: true,
  name: 'mysql',
  create: function(callback) {
    var connection = mysql.createConnection({
      host: config.database.host,
      database: config.database.database,
      user: config.database.user,
      password: config.database.password
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

var Harvester = new pmh.Harvester();

Harvester.on('record', function(record) {
  pool.acquire(function(err, connection) {
    if (err) {
      Harvester.stop();
      console.log(err);
    }
    else {
      var first = new Promise();
      first
      .then(function() {
        var p = new Promise();

        connection.query('SELECT id FROM header WHERE identifier_hash=SHA2(?,256)', [record.identifier], function(err, rows) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve(rows[0].id);
          }
        });

        return p;
      })
      .then(function(id) {
        if (id !== undefined) {
          return id;
        }
        var p = new Promise();

        connection.query('INSERT INTO header (identifier_hash, identifier) VALUES(SHA2(?,256),?)', [record.identifier,record.identifier], function(err, result) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve(result.insertId);
          }
        });

        return p;
      })
      .then(function(id) {
        var p = new Promise();

        connection.query('DELETE FROM dc WHERE id=?', [id], function(err) {
          if (err) {
            p.reject(err);
          }
          else {
            p.resolve(id);
          }
        });

        return p;
      })
      .then(function(id) {
        var values = [];
        for(var term in record.dc) {
          for(var i = 0; i < record.dc[term].length; ++i) {
            values.push('(' + [
                id,
                connection.escape(term),
                i,
                connection.escape(record.dc[term][i])
              ].join(',') + ')');
          }
        }

        if (values.length > 0) {
          var p = new Promise();

          var sql = 'INSERT INTO dc (id,term,pos,value) VALUES ' + values.join(',');
          connection.query(sql, [], function(err) {
            if (err) {
              p.reject(err);
            }
            else {
              p.resolve();
            }
          });

          return p;
        }

        return;
      })

      // release the connection and report
      .then(function() {
        console.log(record.identifier);
        pool.release(connection);
      }, function(err) {
        console.log(err);
        pool.release(connection);
      });

      first.resolve();
    }
  });
});

Harvester.request(endpoint);
