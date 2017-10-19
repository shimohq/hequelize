const Bluebird = require('bluebird')
const debug = require('debug')('hequelize:Model')
const _ = require('lodash')
const HBase = require('hbase-rpc-client')

/*
cf: column family
q: qualifier
*/

class Model {
  /**
   * Creates an instance of Model.
   *
   * @param {string} rowKey the row key
   * @param {object} values like { 'cf:col': 'value }
   * @memberof Model
   */
  constructor (rowKey, values = {}) {
    this.Model = this.constructor
    this.modelOptions = this.constructor.options
    this.tableName = this.modelOptions.tableName
    this.client = this.Model.hequelize.hbaseClient

    this.rowKey = rowKey
    this.data = _.clone(this.modelOptions.defaultData)

    this.modelOptions.columns.forEach(colOption => {
      const col = colOption.name
      Object.defineProperty(this, col, {
        get () {
          const getter = this.modelOptions.columnsObject[col].get
          if (getter) {
            return getter(this.data[col])
          }
          return this.data[col]
        },
        set (value) {
          switch (colOption.type) {
            case 'date':
              this.data[col] = new Date(Number(value))
              break
            case 'number':
              this.data[col] = Number(value)
              break
            case 'string':
              this.data[col] = String(value)
              break
            case 'buffer':
              this.data[col] = Buffer.from(value)
              break
          }

          debug(`set column ${col} to value: ${this.data[col]}`)
        }
      })
    })

    _.forOwn(this.data, (v, k) => {
      const value = values[k]
      if (typeof value !== 'undefined') {
        this[k] = value
      }
    })
  }

  static init (options = {}) {
    this.options = _.defaultsDeep(options, {
      classMethods: {},
      instanceMethods: {}
    })

    const defaultData = this.options.defaultData = {}
    this.options.columnNames = []
    this.options.columnsObject = {}

    this.options.columns.forEach(col => {
      this.options.columnsObject[col.name] = col
      this.options.columnNames.push(col.name)

      if (col.defaultValue) {
        // If default value is function, it will be executed before save method
        defaultData[col.name] = col.defaultValue
      } else {
        defaultData[col.name] = null
      }
    })

    _.forOwn(this.options.classMethods, (fn, fnName) => {
      this[fnName] = fn
    })

    _.forOwn(this.options.instanceMethods, (fn, fnName) => {
      this.prototype[fnName] = fn
    })
  }

  /**
   * Get a row
   *
   * e.g.
   * ```javascript
   * db.File.get('row key', {
   *  columns: ['cf1:col1', 'cf2:cf2']
   * })
   *```
   * @static
   * @param {string} rowKey the row key
   * @param {object} [options = {}]
   * @param {object} [options.columns = all_columns]
   * @returns {Model} instance of this
   * @memberof Model
   */
  static async get (rowKey, options) {
    options = _.defaults(options, {
      columns: this.options.columnNames
    })

    const get = new HBase.Get(rowKey)

    options.columns.forEach(col => {
      const [cf, q] = split(col)
      get.addColumn(cf, q)
    })

    const raw = await this.client.getAsync(this.options.tableName, get)

    if (!raw) {
      return null
    }

    return this.build(rowKey, processRaw(raw))
  }

  static async put (rowKey, values, options) {
    const instance = this.build(rowKey, values, options)
    const columns = []
    const deleteColumns = []
    _.forOwn(values, (value, col) => {
      if (value === undefined || value === null) {
        deleteColumns.push(col)
      } else {
        columns.push(col)
      }
    })
    return instance.save(_.assign({ columns, deleteColumns }, options))
  }

  async save (options) {
    options = _.defaults(options, {
      columns: this.modelOptions.columnNames
    })
    const put = new HBase.Put(this.rowKey)
    options.columns.forEach(col => {
      let value
      const sourceValue = this.data[col]

      const [cf, q] = split(col)
      const colOption = this.modelOptions.columnsObject[col]
      switch (colOption.type) {
        case 'string':
          value = String(sourceValue)
          break
        case 'date':
          value = String(sourceValue.getTime())
          break
        case 'buffer':
          if (sourceValue !== null) {
            value = Buffer.from(sourceValue)
          }
          break
        case 'number':
          value = String(sourceValue)
          break
        default:
          value = String(sourceValue)
      }
      put.add(cf, q, value)
      debug(`save#put add ${cf}:${q} with value ${value}`)
    })

    if (options.deleteColumns.length > 0) {
      const del = new HBase.Delete(this.rowKey)
      options.deleteColumns.forEach(col => {
        const [cf, q] = split(col)
        del.deleteColumns(cf, q)
      })
      await this.client.deleteAsync(this.tableName, del)
    }

    await this.client.putAsync(this.tableName, put)
    return this
  }

  /**
   * Scan
   *
   * @static
   * @param {object} options
   * @param {string} options.startRow the start row key
   * @param {object} [options.stopRow] the stop row key
   * @param {object} [options.limit] the number limit
   * @returns {Array<Model>}
   * @memberof Model
   */
  static async scan (options) {
    let count = 0
    let tmp = {}
    let results = []

    const { startRow, limit = Infinity } = options
    let stopRow = options.stopRow
    // Force to include last record
    if (stopRow) {
      stopRow += '_'
    }

    const scanner = this.client.getScanner(this.options.tableName, startRow, stopRow)
    Bluebird.promisifyAll(scanner)

    while (count < limit && tmp) {
      tmp = await scanner.nextAsync()
      if (tmp && tmp.row) {
        const rowKey = tmp.row.toString()
        results.push(this.build(rowKey, processRaw(tmp)))
        count++
      } else {
        break
      }
    }

    return results
  }

  static build (rowKey, values, options) {
    return new this(rowKey, values, options)
  }

  toJSON () {
    return {
      rowKey: this.rowKey,
      data: this.data
    }
  }
}

function processRaw (raw) {
  return raw.columns.reduce((acc, col) => {
    acc[`${col.family}:${col.qualifier}`] = col.value
    return acc
  }, {})
}

function split (col) {
  return col.split(':')
}

module.exports = Model
