'use strict'
// system
let fs = require('fs')
let http = require('http')

// npm
let websock = require('websock')
let Hjson= require('hjson')
let rethinkdb = require('rethinkdb')
let moment = require('moment')

// local
let db = require('./lib/db')

let config = Hjson.parse(fs.readFileSync('./config.hjson', 'utf8'))

rethinkdb
  .connect({db: 'cointhink'})
  .then(
    function(conn) {
      db.tableList(conn)
      return conn
    }, function(err) {
      console.log('no rethinkdb available')
      console.log(err)
    })
  .then(
    function(conn) {
      console.log('listen websocket :'+config.websocket.listen_port)
      websock.listen(config.websocket.listen_port, function(socket) {
        console.log('websocket connected from ', socket.remoteAddress)
        socket.on('message', (msg) => {
            ddispatch(msg, conn)
              .then((out) => {console.log('->', out); socket.send(out) } )
          })
        socket.on('close', () => console.log('websocket close') )
      })

      http.createServer(function(request, response) {
        console.log('http connected from')
        ddispatch(request.body, conn)
      })
    })


function ddispatch(msg, conn) {

  try {
    var rpc = JSON.parse(msg)
    console.log('<-ws', JSON.stringify(rpc))
    return dispatch(rpc)
  } catch (e){
    console.log('<-bad', msg)
  }

    function dispatch(rpc) {
      if (rpc.method == "orderbook") {
        let now = new Date()
        let base = rpc.params.base.toUpperCase()
        let quote = rpc.params.quote.toUpperCase()
        let hours = parseFloat(rpc.params.hours)

        return sendBooks(base, quote, 1000*60*60*hours)

        function sendBooks(base, quote, duration) {
          let early = [base, quote, new Date(now-duration)]
          let late = [base, quote, now]
          return rethinkdb
            .table('orderbooks')
            .orderBy({index: rethinkdb.desc('base-quote-date')})
            .between(early, late)
            .run(conn)
            .then(function(cursor){
              cursor
              .each(function(err, book){
                book.asks = [ book.asks[0] ]
                book.bids = [ book.bids[0] ]
                console.log('orderbook', book.exchange, book.market.base, book.market.quote,
                                         book.asks, book.bids)
                return obsend('orderbook', book)
              })
            })
        }
      }

      if (rpc.method == "exchanges") {
        return rethinkdb
        .table('exchanges')
        .run(conn)
        .then(function(cursor){
          return cursor
          .each(function(err,exchange){
            console.log('exchange lookup:', exchange.id)
            return lastOrderbook(exchange)
              .run(conn)
              .then(function(cursor){
                return cursor
                  .toArray()
                  .then(function(lastbooks){
                    let stat = {id: exchange.id, markets: [] }
                    if (lastbooks.length > 0) {
                      let lastDate = lastbooks[0].date
                      stat.date = lastDate
                      console.log('lastDate', exchange.id, lastDate)
                      return marketCluster(exchange,
                                           moment(lastDate).subtract(45, 'seconds').toDate(),
                                           new Date())
                        .run(conn)
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
              .then(function(exchange){
                console.log('exchange result:', exchange)
                return obsend('exchange', exchange)
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
        return JSON.stringify(obj, null, 2)
      }
    }
}
