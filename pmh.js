var http = require('http')
  , https = require('https')
  , url = require('url')
  , expat = require('node-expat')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
;

var Harvester = function(options) {
  EventEmitter.call(this);

  if (!options) options = {};

  this.headers = options.headers ? options.headers : {};

  // cache recent rtokens to spot unchanging tokens
  this.tokens = [];

  // number of redirects followed
  this.redirect_depth = 0;

  this.retries = 5;
};
util.inherits(Harvester, EventEmitter);
exports.Harvester = Harvester;

exports.dcTerms = function() {
	return [
		"contributor",
		"coverage",
		"creator",
		"date",
		"description",
		"format",
		"header",
		"identifier",
		"language",
		"publisher",
		"relation",
		"rights",
		"source",
		"subject",
		"title",
		"type"
	];
};

Harvester.prototype.stop = function() {
  if (this.parser) this.parser.stop();
  this.paused = true;
};

Harvester.prototype.request = function(endpoint, options) {
  var _this = this;
  this.endpoint = url.parse(endpoint);
  if (!options) options = {};

  if (this.paused) {
    return;
  }

  var parser = new expat.Parser("UTF-8");
  this.parser = parser; // used to pause

  var q = url.parse(this.endpoint.format().replace(/\?.*/, ''));

  q.query = {};
  q.query.verb = 'ListRecords';

  if (options.resumptionToken) {
    q.query.resumptionToken = options.resumptionToken;
  }
  else {
    q.query.metadataPrefix = 'oai_dc';
  }

  var path = [];
  var text = '';
  var record = {
    status: null,
    dc: {},
    setSpec: []
  };
  var resumptionToken;
  parser.on('startElement', function(name, attrs) {
    _this.emit('startElement', name, attrs);

    var localName = name.replace(/^.+:/,'');
    path.push(localName);
    //console.log('<' + path.join('/') + '>');
    text = '';
    // only valid status is 'deleted'
    if (path.join('/') === 'OAI-PMH/ListRecords/record/header' && attrs.status === 'deleted') {
      record.status = attrs.status;
    }
  });
  parser.on('text', function(t) {
    _this.emit('text', t);

    text += t;
  });
  parser.on('endElement', function(name, attrs) {
    _this.emit('endElement', name, attrs);

    var localName = name.replace(/^.+:/,'');
    if (path.join('/') === 'OAI-PMH/ListRecords/record/header/identifier') {
      record.identifier = text.replace(/\s+/, '');
    }
    else if (path.join('/') === 'OAI-PMH/ListRecords/record/header/datestamp') {
      record.datestamp = text.replace(/\s+/, '');
    }
    else if (path.join('/') === 'OAI-PMH/ListRecords/record/header/setSpec') {
      record.setSpec.push(text.replace(/\s+/, ''));
    }
    else if (path.join('/') === 'OAI-PMH/ListRecords/record') {
      _this.emit('record', record);
      record = {
        status: null,
        dc: {},
        setSpec: []
      };
    }
    else if (path.join('/') === 'OAI-PMH/ListRecords/resumptionToken') {
      resumptionToken = text.replace(/\s+/, '');
    }
    path.pop();
    if (path.join('/') === 'OAI-PMH/ListRecords/record/metadata/dc') {
      if (!record.dc[localName]) record.dc[localName] = [text];
      else record.dc[localName].push(text);
    }
    text = '';
  });
  parser.on('error', function(e) {
    _this.emit('error', e);
  });

  _this.emit('request', q);
  var req = (q.protocol === 'http:' ? http : https).request(q.format(), function(res) {
    _this.emit('response', req, res);
    switch(res.statusCode) {
      case 200:
        _this.redirect_depth = 0;
        res
        .on('data', function(data) {
          if (_this.paused) {
            req.abort();
            return;
          }
          _this.emit('response_data', data);
          parser.write(data);
        })
        .on('end', function() {
          // tidy up parser
          parser.end();
          _this.parser = undefined;
     
          _this.emit('response_end', req, res);

          // resume the partial list with the given token
          if (resumptionToken !== undefined && resumptionToken.length > 0) {
            var next = function() {
              _this.request(endpoint, {
                resumptionToken: resumptionToken
              });
            };
            if (_this.listeners('resume').length > 0) {
              _this.emit('resume', {
                  resumptionToken: resumptionToken
                }, next);
            }
            else {
              next();
            }
          }

          // nothing left to do
          else {
            _this.emit('end');
          }
        });
        break;
      // redirect
      case 301:
      case 302:
        var location = res.headers['location'];
        location = url.resolve(q.format(), location);
        if (_this.redirect_depth++ > 5) {
          _this.emit('error', 'Exceeded redirect limit: ' + location);
          break;
        }
        _this.emit('redirect', location);
        _this.request(location, options);
        break;
      // retry-after
      case 503:
        _this.redirect_depth = 0;
        var seconds = res.headers['retry-after'] + 0;
        if (seconds > 0 && seconds < 600) {
          _this.emit('sleep', seconds * 1000);
          setTimeout(function() {
            _this.request(endpoint, options);
          }, (seconds + 5) * 1000);
          break;
        }
      default:
        _this.emit('error', res.statusCode + ': ' + q.format());
    }

  }, this.headers);
  req.on('error', function(e) {
    _this.emit('error', e);
  });
  req.end();
};
