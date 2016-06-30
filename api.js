'use strict'
let fs = require('fs')

let websock = require('websock')
let Hjson= require('hjson')
let rethinkdb = require('rethinkdb')

//var config = Hjson.parse(fs.readFileSync('./config.hjson'))
let config = JSON.parse(fs.readFileSync('./config.hjson'))
console.log('config', config)

websock.listen(config.websocket.listen_port, do_connect);

function do_connect(socket) {
  console.log('ws_connect')

  socket.on('message', function(data) {
    console.log('<-ws '+data)
  });

  socket.on('close', function() {
    console.log('websockets close');
  });
}

