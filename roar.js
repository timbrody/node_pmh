var 
  poolModule = require('generic-pool')
  , pmh = require('./pmh')
  , mysql = require('mysql')
  , promise = require('node-promise')
  , Promise = require('node-promise').Promise
  , util = require('util')
  , http = require('http')
;

var roar = function(opts) {
	this.opts = opts;
	this.initialize();
};
exports = module.exports = function(opts) {
	return new roar(opts);
};

roar.prototype.initialize = function() {
};

roar.prototype.log = function(err) {
  console.log(err);
};

roar.prototype.list = function(callback) {
  var url = require('url').parse('http://roar.eprints.org/cgi/search/celestial/advanced');
  url.query = {
    dataset: 'celestial',
    _action_export: 1,
    output: 'Ids',
    celestialid: '1..'
  };
  var req = http.request(url.format(), function(res) {
    if (res.statusCode !== 200) {
      console.log('Error from ' + url.format() + ': ' + res.statusCode);
      return;
    }
    var buffer = '';
    res.on('data', function(data) {
      buffer += data;
    });
    res.on('end', function() {
      var ids = buffer.split('\n');
      for(var i=0; i<ids.length; ++i) {
        var id = new Number(ids[i]);
        callback(id);
      }
    });
  });
  req.end();
};

roar.prototype.get = function(celestialid, callback) {
  var url = require('url').parse('http://roar.eprints.org/id/celestial/' + celestialid);
  url.headers = {
    Accept: 'application/json'
  };
  var req = http.request(url, function(res) {
    if (res.statusCode !== 200) {
      console.log('Error from ' + url.format() + ': ' + res.statusCode);
      return;
    }
    var buffer = '';
    res.on('data', function(data) {
      buffer += data;
    });
    res.on('end', function() {
      callback(JSON.parse(buffer));
    });
  });
  req.end();
};

roar.prototype.update = function(repo) {
  var url = require('url').parse('http://roar.eprints.org/id/celestial/' + repo.celestialid);
  url.method = 'PUT';
  url.auth = [
      this.opts.roar.api.user,
      this.opts.roar.api.password
    ].join(':');
  url.headers = {
    'Content-Type': 'application/json'
  };
  var req = http.request(url, function(res) {
    if (res.statusCode !== 204) {
      console.log('Error from ' + url.format() + ': ' + res.statusCode);
    }
  });
  req.end(JSON.stringify(repo));
};
