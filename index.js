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
    try {
      var rpc = JSON.parse(msg)
      console.log('<-ws', JSON.stringify(rpc))
      dispatch(rpc)
    } catch (e){
      console.log('<-bad', msg)
    }

    function dispatch(rpc) {
      if (rpc.method == "orderbook") {
        let now = new Date()
        let base = rpc.params.base.toUpperCase()
        let quote = rpc.params.quote.toUpperCase()
        let hours = parseInt(rpc.params.hours)
        let early = [base, quote, new Date(now-1000*60*60*hours)]
        let late = [base, quote, now]
        return rethinkdb
        .table('orderbooks')
        .orderBy({index: rethinkdb.desc('base-quote-date')})
        .between(early, late)
        .run(db)
        .then(function(cursor){
          cursor
          .each(function(err, book){
            book.asks = [ book.asks[0] ]
            book.bids = [ book.bids[0] ]
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
            return rethinkdb
            .table('orderbooks')
            .orderBy({index: rethinkdb.desc('exchange-date')})
            .between([exchange.id, new Date(0)], [exchange.id, new Date()])
            .limit(1)
            .run(db)
            .then(function(cursor){
              cursor
              .each(function(err, lastbook){
                let exchangeStat = {exchange: exchange.id, lastQuote: lastbook.date}
                obsend('exchange', exchangeStat)
              })
            })
          })
        })
      }

      function obsend(type, object) {
        let obj = { type: type, object: object}
        console.log(rpc.method, rpc.params, '->', JSON.stringify(obj, null, 2))
        socket.send(JSON.stringify(obj))
      }
    }
  })

  socket.on('close', function() {
    console.log('websocket close')
  })
}

