'use strict'
let pgp = require('pg-promise')()

module.exports = (function(){
  var o = {}
  let db

  o.setup = function(config) {
    db = pgp(config)
  }

  o.orderbooks = function (base, quote, now, duration) {
    let sql = 'SELECT * from orderbooks where ' +
              'base = ${base} and quote = ${quote} and ' +
              'date > ${time}::timestamp'
    let time = new Date(now - duration)
    let params = {
                  base: base,
                  quote: quote,
                  time: time
                 }
    return db.query(sql, params)

      // cursor
      // .each(function(err, book){
      //   book.asks = [ book.asks[0] ]
      //   book.bids = [ book.bids[0] ]
      //   console.log('orderbook', book.exchange, book.market.base, book.market.quote,
      //                            book.asks, book.bids)
      //   cb(book)
      // })
  }

  return o

})()
