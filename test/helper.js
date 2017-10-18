const Hequelize = require('../lib/Hequelize')
const hbase = new Hequelize({
  zkHosts: 'localhost:2181'
})

// hbase shell: create 'hbasetest', 'cf1', 'cf2'

function createHb (options = {}) {
  hbase.define('Test', Object.assign({
    tableName: 'hbasetest',
    columns: [{
      name: 'cf1:string',
      type: 'string'
    }, {
      name: 'cf2:date',
      type: 'date'
    }, {
      name: 'cf2:number',
      type: 'number'
    }, {
      name: 'cf2:buffer',
      type: 'buffer'
    }],
    classMethods: {
      testClassMethod () {

      }
    },
    instanceMethods: {
      testInstanceMethod () {
        return this
      }
    }
  }, options))

  return hbase
}

exports.createHb = createHb
