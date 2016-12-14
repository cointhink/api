'use strict'
//var pgp = require('pg-promise')({query: x=>console.log(x.query)})
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
    console.log(sql, params)
    return db.query(sql, params)
      .then(books => Promise.all(books.map(ob => {
        return Promise.all([
            o.offers(ob.id, "bid"),
            o.offers(ob.id, "ask")
          ])
        .then(offerz => {
          // rejigger the format
          offerz = offerz.map(of => [of[0].price, parseFloat(of[0].quantity)])
          return {
            date: ob.date,
            exchange: ob.exchangeId,
            market: {base: ob.base, quote: ob.quote},
            bids: [offerz[0]],
            asks: [offerz[1]]
          }
        })
      })))


      // cursor
      // .each(function(err, book){
      //   book.asks = [ book.asks[0] ]
      //   book.bids = [ book.bids[0] ]
      //   console.log('orderbook', book.exchange, book.market.base, book.market.quote,
      //                            book.asks, book.bids)
      //   cb(book)
      // })
  }

  o.offers = function(orderbookId, bidAsk) {
    let sql = 'SELECT * from offers where "orderbookId" = $1 and "bidAsk" = $2 limit 1'
    let params = [orderbookId, bidAsk]
    return db.query(sql, params)
  }

  o.exchanges = function() {
    let sql = 'SELECT * from exchanges'
    let params = {
                 }
    return db.query(sql, params)
      .then(exchanges => exchanges.map(e => {
        return {
          id: e.name,
          markets: [],
          date: new Date()
        }
      }))
  }

  return o

})()
