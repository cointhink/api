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
                  time: time.toISOString()
                 }
    return db.query(sql, params)
      .then(books => books.map(o => {
        // rejigger the format
        delete o.id
        o.exchange = o.exchangeId
        delete o.exchangeId
        o.market = {base: o.base, quote: o.quote}
        delete o.base
        delete o.quote
        o.bids = [["1", 1]]; o.asks = [["1", 1]];
        return o
      }))


      // cursor
      // .each(function(err, book){
      //   book.asks = [ book.asks[0] ]
      //   book.bids = [ book.bids[0] ]
      //   console.log('orderbook', book.exchange, book.market.base, book.market.quote,
      //                            book.asks, book.bids)
      //   cb(book)
      // })
  }

  o.offers = function(orderbookId) {
  }

  o.exchanges = function() {
    let sql = 'SELECT * from exchanges'
    let params = {
                 }
    return db.query(sql, params)
      .then(exchanges => exchanges.map(e => {
        delete e.id
        delete e.marketsJs
        delete e.orderbookJs
        delete e.offerJs
        return e
      }))
  }

  return o

})()
