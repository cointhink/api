'use strict'
let fs = require('fs')

let websock = require('websock')
let Hjson= require('hjson')
let rethinkdb = require('rethinkdb')

//var config = Hjson.parse(fs.readFileSync('./config.hjson'))
let config = JSON.parse(fs.readFileSync('./config.hjson'))
console.log('config', config)


rethinkdb.connect({db: 'cointhink'})
.then(function(conn){
  rethinkdb
  .tableList()
  .run(conn)
  .then(function(list){
    console.log(list)
  })

  websock.listen(config.websocket.listen_port, function(socket) {
    do_connect(socket, conn)
  })

})


function do_connect(socket, db) {
  console.log('websocket open')

  socket.on('message', function(msg) {
    var rpc = JSON.parse(msg)
    console.log('<-ws '+ JSON.stringify(rpc))

    if (rpc.method == "orderbook") {
      return rethinkdb
      .table('orderbooks')
      .orderBy({index: rethinkdb.desc('date')})
      .limit(1)
      .run(db)
      .then(function(cursor){
        cursor
        .toArray()
        .then(function(books){
          let book = books[0]
          console.log(rpc.method, rpc.params, '->', book.date, book.market)
          socket.send(JSON.stringify(book))
        })
      })
    }

    if (rpc.method == "exchanges") {
      return rethinkdb
      .table('exchanges')
      .run(db)
      .then(function(cursor){
        cursor
        .toArray()
        .then(function(exchanges){
          exchanges = exchanges.map(function(ex){return {name: ex.id}})
          console.log(rpc.method, '->', exchanges)
          socket.send(JSON.stringify(exchanges))
        })
      })
    }
  })

  socket.on('close', function() {
    console.log('websocket close')
  })
}

