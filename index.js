'use strict'
let fs = require('fs')

let websock = require('websock')
let Hjson= require('hjson')
let rethinkdb = require('rethinkdb')
let moment = require('moment')

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
          return cursor
          .each(function(err,exchange){
            console.log('exchange', exchange.id)
            return lastOrderbook(exchange)
              .run(db)
              .then(function(cursor){
                return cursor
                  .toArray()
                  .then(function(lastbooks){
                    let stat = {id: exchange.id, markets: [] }
                    console.log('lastbooks', exchange.id, lastbooks)
                    if (lastbooks.length > 0) {
                      let lastDate = lastbooks[0].date
                      stat.date = lastDate
                      console.log('lastDate', exchange.id, lastDate)
                      return marketCluster(exchange,
                                           moment(lastDate).subtract(45, 'seconds').toDate(),
                                           new Date())
                        .run(db)
                        .then(function(cursor){
                          return cursor
                            .toArray()
                            .then(function(lastbooks){
                              console.log(exchange.id, 'books', lastbooks.length)
                              lastbooks.forEach(function(book){
                                stat.markets.push(book.market)
                              })
                              return stat
                            })
                        })
                    } else {
                      return stat
                    }
                })
              })
              .then(function(exchanges){
                obsend('exchange', exchanges)
              })

          })
        })
      }

      function lastOrderbook(exchange) {
        return marketCluster(exchange, new Date(0), new Date())
               .limit(1)
      }

      function marketCluster(exchange, startDate, lastDate) {
        return rethinkdb
          .table('orderbooks')
          .orderBy({index: rethinkdb.desc('exchange-date')})
          .between([exchange.id, startDate],
                   [exchange.id, lastDate])
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

