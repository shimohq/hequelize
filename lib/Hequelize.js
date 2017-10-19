const _ = require('lodash')
const Model = require('./Model')
const HBase = require('hbase-rpc-client')
const Bluebird = require('bluebird')

class Hequelize {
  /**
   * constructor
   *
   * @param {object}        [options]
   * @param {string|array}  [options.zookeeperHosts]
   * @param {string}        [options.zookeeperRoot = '/hbase']
   * @param {int}           [options.zookeeperReconnectTimeout = 20000]
   * @param {string}        [options.rootRegionZKPath = '/meta-region-server']
   * @param {int}           [options.rpcTimeout = 30000]
   * @param {int}           [options.callTimeout = 5000]
   * @param {boolean}       [options.tcpNoDelay = false]
   * @param {boolean}       [options.tcpKeepAlive = true]
   * @param {string}        [options.realUser]
   * @param {string}        [options.effectiveUser]
   *
   * @param {object}        [options.logger = console]
   */
  constructor (options = {}) {
    this.options = _.defaultsDeep(options, {
      zookeeperHosts: ['localhost:2181']
    })

    const zkHosts = this.options.zookeeperHosts
    if (typeof zkHosts === 'string') {
      this.options.zookeeperHosts = zkHosts.split(',')
    }

    this.hbaseClient = HBase(this.options)

    Bluebird.promisifyAll(this.hbaseClient)

    this.hbaseClient.on('error', error => {
      console.error('hequelize error:', error)
    })
  }

  /**
   * Define a new model
   *
   * @param {string} modelName the model name, like 'File', 'Resource'
   * @param {object} [options]
   * @param {object} [options.tableName] table name in hbase
   * @param {object} [options.columns] column define, like [{ name: 'cf:col', type: 'string' }]
   * @param {object} [options.classMethods] model class method
   * @param {object} [options.instanceMethods] instance class methods
   * @memberof Hequelize
   */
  define (modelName, options) {
    const model = this[modelName] = class extends Model {}
    model.modelName = modelName
    model.hequelize = this
    model.client = this.hbaseClient
    model.init(options)
  }
}

module.exports = Hequelize
