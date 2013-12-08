var pmh = require('./pmh')
  , poolModule = require('generic-pool')
  , mysql = require('mysql')
  , config = require('./config.json')
  , Promise = require('node-promise').Promise
  , express = require('express')
;

var app = express();

var celestial = require('./celestial')(config);
var roar = require('./roar')(config);

app.get('/repository', function(req, res) {
	res.send(200, celestial.currentState());
});
app.get('/repository/:id/status', function(req, res) {
	res.send(200, celestial.currentState()[req.params.id]);
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

function harvestAll() {
  roar.list(function(celestialid) {
if (celestialid != 90) return;
    // in progress
    if (celestial.currentState()[celestialid]) {
      return;
    }
    roar.get(celestialid, function(repo) {
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
          type: 'error',
          message: '' + err
        }];
        roar.update(repo);
      })
      ;
    });
  });
}

// check every 10 minutes
setInterval(harvestAll, 600000);
harvestAll();
