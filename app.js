var pmh = require('./pmh')
  , poolModule = require('generic-pool')
  , mysql = require('mysql')
  , config = require('./config.json')
  , Promise = require('node-promise').Promise
  , express = require('express')
  , app = express()
  , server = require('http').createServer(app)
  , io = require('socket.io').listen(server)
;

app.use(express.static(__dirname + '/public'));

var celestial = require('./celestial')(config);
var roar = require('./roar')(config);

var repositories = {};
var currentState = {};

app.get('/repository', function(req, res) {
  var cState = {};
  for(var celestialid in repositories) {
    cState[celestialid] = repositories[celestialid];
  }
  for(var celestialid in currentState) {
    cState[celestialid] = currentState[celestialid];
  }
	res.send(200, cState);
});
app.get('/repository/:id/status', function(req, res) {
	res.send(200, celestial.currentState()[req.params.id]);
});
app.post('/repository/:id/start', function(req, res) {
  var celestialid = req.params.id;
  if (!celestial.currentState()[celestialid]) {
    roar.get(celestialid, function(repo) {
      startHarvest(repo);
    });
  }
	res.send(200, 'Commencing harvesting of ' + req.query.endpoint);
});
app.post('/repository/:id/stop', function(req, res) {
	celestial.stop(req.params.id);
	res.send(200, 'Stopping harvesting of ' + req.query.endpoint);
});

//server.listen(config.controller.port, 'localhost');
server.listen(config.controller.port);
console.log('Listening on port ' + config.controller.port);

io.sockets.on('connection', function(socket) {
  for(var celestialid in repositories) {
    socket.emit('state', repositories[celestialid]);
  }
  for(var celestialid in currentState) {
    socket.emit('state', currentState[celestialid]);
  }
});
celestial.on('state', function(state) {
  currentState[state.celestialid] = state;
  io.emit('state', state);
  switch (state.status) {
    case 'finished':
    case 'error':
      delete currentState[state.celestialid];
      break;
  }
});

function startHarvest(repo) {
  // find oai_dc
  var oai_dc;
  for(var i=0; repo.formats && i<repo.formats.length; ++i) {
    if (repo.formats[i].prefix === 'oai_dc') {
      oai_dc = repo.formats[i];
    }
  }
  if (oai_dc === undefined) {
    oai_dc = {
      namespace: 'http://www.openarchives.org/OAI/2.0/oai_dc/',
      schema: 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
      prefix: 'oai_dc'
    };
    if (!repo.formats) repo.formats=[oai_dc];
    else repo.formats.push(oai_dc);
  }
  // harvested in last 7 days?
  if (oai_dc.harvest && oai_dc.harvest.length) {
    var staleTime = new Date();
    staleTime -= 1000 * 60 * 60 * 24 * 7;
    staleTime = new Date(staleTime);
    var repoTime = new Date(oai_dc.harvest);
    if (repoTime.getTime() >= staleTime.getTime()) {
      return;
    }
  }
  // commence harvest
  var startTime = new Date();
  if (!repo.datestamp) {
    repo.datestamp = startTime.toISOString();
  }
  var harvester = celestial.start(repo);
  harvester
  .on('end', function() {
    repo.messages = [];
    oai_dc.harvest = startTime.toISOString();
    oai_dc.token = null;
    roar.update(repo);
  })
  .on('resume', function(opts) {
    oai_dc.token = opts.resumptionToken;
    roar.update(repo);
  })
  .on('error', function(err) {
    // clear token on server error (we keep the token to cope with our own
    // errors)
    oai_dc.token = null;
    repo.messages = [{
      timestamp: new Date().toISOString(),
      type: 'error',
      message: '' + err
    }];
    roar.update(repo);
  })
  ;
}

function harvestAll() {
  repositories = {};
  roar.list(function(celestialid) {
    repositories[celestialid] = {
      celestialid: celestialid,
      status: 'sleeping'
    };
//if (celestialid != 11) return; //ugent
//if (celestialid != 2) return; //cogprints
//if (celestialid != 90) return; //eprints.soton
if (celestialid != 1249) return;
    // in progress
    if (celestial.currentState()[celestialid]) {
      return;
    }
    roar.get(celestialid, function(repo) {
      startHarvest(repo);
    });
  });
}

// check every 10 minutes
setInterval(harvestAll, 600000);
harvestAll();
