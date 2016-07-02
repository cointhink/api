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
  //exchange: 'poloniex',
  //market: { base: 'XMR', quote: 'XDN' },

    if (rpc.method == "orderbook") {
      let now = new Date()
      let base = rpc.params.base.toUpperCase()
      let quote = rpc.params.quote.toUpperCase()
      let days = parseInt(rpc.params.days)
      let early = [base, quote, new Date(now-1000*60*60*24*days)]
      let late = [base, quote, now]
      return rethinkdb
      .table('orderbooks')
      .orderBy({index: rethinkdb.desc('base-quote-date')})
      .between(early, late)
      .run(db)
      .then(function(cursor){
        cursor
        .each(function(err, book){
          obsend('orderbook', book)
        })
      })
    }

    if (rpc.method == "exchanges") {
      return rethinkdb
      .table('exchanges')
      .run(db)
      .then(function(cursor){
        cursor
        .each(function(err, exchange){
          obsend('exchange', exchange)
        })
      })
    }

    function obsend(type, object) {
      let obj = { type: type, object: object}
      console.log(rpc.method, rpc.params, '->', obj)
      socket.send(JSON.stringify(obj))
    }
  })

  socket.on('close', function() {
    console.log('websocket close')
  })
}

