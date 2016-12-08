'use strict'
// system
let fs = require('fs')
let http = require('http')

// npm
let websock = require('websock')
let Hjson= require('hjson')
let moment = require('moment')

// local
let db = require('./lib/db')
let config = Hjson.parse(fs.readFileSync('./config.hjson', 'utf8'))

db.setup(config.cockroach)
mainloop()

function mainloop() {
  console.log('listen websocket :'+config.websocket.listen_port)
  websock.listen(config.websocket.listen_port, function(socket) {
    console.log('websocket connected from ', socket.remoteAddress)
    socket.on('message', (msg) => {
        ddispatch(msg)
          .then(answers => answers.forEach(answer => socket.send(answer)))
      })
    socket.on('close', () => console.log('websocket close') )
  })

  http.createServer(function(request, response) {
    console.log('http connected from')
    ddispatch(request.body, conn)
  })
}


function ddispatch(msg) {
  try {
    var rpc = JSON.parse(msg)
    console.log('<-ws', JSON.stringify(rpc))
    return dispatch(rpc)
  } catch (e){
    console.log('<-bad', msg)
    return Promise.resolve({err: e})
  }

    function dispatch(rpc) {
      console.log('method:', rpc.method)
      if (rpc.method == "orderbook") {
        let now = new Date()
        let base = rpc.params.base.toUpperCase()
        let quote = rpc.params.quote.toUpperCase()
        let hours = parseFloat(rpc.params.hours)

        return db
          .orderbooks(base, quote, new Date(), 1000*60*60*hours)
          .then(books => books.map(book => obsend('orderbook', book)))
          .catch(x=>console.log('ob err', x))
      } else if (rpc.method == "exchanges") {
        return db.exchanges()
          .then(exchanges => {
            return exchanges.map(exchange => {
              //let lastbooks = lastOrderbook(exchange)
                    // let stat = {id: exchange.id, markets: [] }
                    // if (lastbooks.length > 0) {
                    //   let lastDate = lastbooks[0].date
                    //   stat.date = lastDate
                    //   console.log('lastDate', exchange.id, lastDate)
                    //   return marketCluster(exchange,
                    //                        moment(lastDate).subtract(45, 'seconds').toDate(),
                    //                        new Date())
                    //           console.log(exchange.id, 'books', lastbooks.length)
                    //           lastbooks.forEach(function(book){
                    //             stat.markets.push(book.market)
                    //           })
                return obsend('exchange', exchange)
          })
        })
      } else {
        return Promise.resolve({err: "unknown command"})
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
        let json = JSON.stringify(obj, null, 2)
        return json
      }
    }
}
