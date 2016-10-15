'use strict'
let rethinkdb = require('rethinkdb')

module.exports = (function(){
  var o = {}

  o.setup = function() {
  }

  o.tableList = function() {
    rethinkdb
      .tableList()
      .run(conn)
      .then(function(list){
        console.log(list)
      })
  }

})()
