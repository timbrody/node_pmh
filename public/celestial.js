$(function() {
  var container = $('#status');
  var table = $('<table/>');
  container.html(table);
  $('<tr><th>Celestial ID</th><th>Status</th><th>Start</th><th>Records</th><th>Requests</th><th>Request</th></tr>').appendTo(table);

  var endpoint = location.protocol + '//' + location.host;
  var socket = io.connect(endpoint);
  socket.on('connection', function() {
    console.log('connection');
  });
  socket.on('state', function(state) {
    var row = $('#celestial_' + state.celestialid);
    if (row.length === 0) {
      row = $('<tr/>', {
        id: 'celestial_' + state.celestialid
      });
      row.appendTo(table);
    }
    row.empty();
    [
      state.celestialid,
      state.status,
      state.start ? new Date(state.start).toISOString() : '',
      state.states,
      state.requests,
      state.request
    ].map(function(value) {
      $('<td />', {
        text: value
      }).appendTo(row);
    });
    switch(state.status) {
      case 'sleeping':
        var td = $('<td><button data-celestialid="'+state.celestialid+'">Start</button></td>');
        td.appendTo(row);
        var button = td.find('button');
        button.click(function() {
          $.ajax({
            url: endpoint + '/repository/' + state.celestialid + '/start',
            method: 'post'
          });
        });
        break;
      default:
        break;
    }
  });
});
