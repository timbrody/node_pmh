var pmh = require('./pmh')
  , poolModule = require('generic-pool')
  , mysql = require('mysql')
  , config = require('./config.json')
  , Promise = require('node-promise').Promise
  , express = require('express')
  , celestial = require('./celestial')
;

var app = express();

var celestial = celestial(config.database);

app.get('/repository/:id/status', function(req, res) {
	res.send(200, celestial.getStatus(req.params.id));
});
app.post('/repository/:id/start', function(req, res) {
	celestial.start(req.params.id, req.query.endpoint);
	res.send(200, 'Commencing harvesting of ' + req.query.endpoint);
});
app.post('/repository/:id/stop', function(req, res) {
	celestial.stop(req.params.id);
	res.send(200, 'Stopping harvesting of ' + req.query.endpoint);
});

app.listen(config.controller.port, 'localhost');
console.log('Listening on port ' + config.controller.port);
