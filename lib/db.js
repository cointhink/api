'use strict'
let pg = require('pg')

module.exports = (function(){
  var o = {}
  let db

  o.setup = function(config) {
    db = pg.Client(config.cockroach)
  }

  o.orderbooks = function (base, quote, duration, cb) {
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
          cb(book)
        })
      })
  }

  return o

})()
