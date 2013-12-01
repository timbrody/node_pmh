var http = require('http')
  , url = require('url')
  , expat = require('node-expat')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
;

var Harvester = function(options) {
  EventEmitter.call(this);

  if (!options) options = {};

  this.flush = options.flush ? options.flush : 300;

  // state variables, useful for monitoring
  this.state = {};
};
util.inherits(Harvester, EventEmitter);
exports.Harvester = Harvester;

Harvester.prototype.stop = function() {
  if (this.parser) this.parser.stop();
};

Harvester.prototype.resume = function() {
  if (this.parser) this.parser.resume();
};

Harvester.prototype.request = function(endpoint, options) {
  var _this = this;
  this.endpoint = url.parse(endpoint);
  if (!options) options = {};

  var parser = new expat.Parser("UTF-8");
  this.parser = parser; // used to pause

  var q = url.parse(this.endpoint.format());

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
    dc: {}
  };
  var resumptionToken;
  parser.on('startElement', function(name, attrs) {
    _this.emit('startElement', name, attrs);

    var localName = name.replace(/^.+:/,'');
    path.push(localName);
    //console.log('<' + path.join('/') + '>');
    text = '';
  });
  parser.on('text', function(t) {
    _this.emit('text', t);

    text += t;
  });
  parser.on('endElement', function(name, attrs) {
    _this.emit('endElement', name, attrs);

    var localName = name.replace(/^.+:/,'');
    if (path.join('/') === 'OAI-PMH/ListRecords/record/header/identifier') {
      record.identifier = text;
    }
    else if (path.join('/') === 'OAI-PMH/ListRecords/record') {
      _this.emit('record', record);
      record = {
        dc: {}
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

  console.log(q.format());
  _this.state.status = 'Requesting';
  var req = http.request(q.format(), function(res) {
    _this.state.status = 'Receiving';
    res.on('data', function(data) {
      _this.state.status = 'Parsing';
      parser.write(data);
      _this.state.status = 'Receiving';
    });
    res.on('end', function() {
      // tidy up parser
      parser.end();
      _this.parser = undefined;
 
      _this.state.status = 'Finished';

      // resume the partial list with the given token
      if (resumptionToken) {
        var next = function() {
          _this.request(endpoint, {
            resumptionToken: resumptionToken
          });
        };
        if (_this.listeners('resume').length > 0) {
          _this.emit('resume', next);
        }
        else {
          next();
        }
      }

      // nothing left to do
      else {
        _this.state = {};
        _this.emit('end');
      }
    });
  });
  req.on('error', function(e) {
    _this.emit('error', e);
  });
  req.end();

  this.state.request = req;
};
