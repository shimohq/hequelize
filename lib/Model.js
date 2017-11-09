const Bluebird = require('bluebird')
const _ = require('lodash')
const HBase = require('hbase-rpc-client')

/*
cf: column family
q: qualifier
*/

class Model {
  static init (options = {}) {
    _.assign(this, _.defaultsDeep(options, {
      classMethods: {}
    }))

    this.columnNames = []
    this.columnsObject = {}

    this.columns.forEach(col => {
      this.columnsObject[col.name] = col
      this.columnNames.push(col.name)
    })

    _.forOwn(this.classMethods, (fn, fnName) => {
      this[fnName] = fn
    })
  }

  /**
   * Get a row
   *
   * @param {string} rowKey the row key
   * @param {object} [options = {}]
   * @param {object} [options.columns = all_columns]
   *
   * @returns {object} return object
   */
  static async get (rowKey, options) {
    options = _.defaults(options, {
      columns: this.columnNames
    })

    options.columns = this.conformColumns(options.columns)

    const get = new HBase.Get(rowKey)
    options.columns.forEach(col => {
      const [cf, q] = split(col)
      get.addColumn(cf, q)
    })

    const raw = await this.client.getAsync(this.tableName, get)

    return this.formatAfterGet(raw, options)
  }

  /**
   * mget
   *
   * @param {Array<string>} rowKeys the row key array
   * @param {object} [options]
   * @param {Array<string>} [options.columns = all_columns] columns
   *
   * @return {Array<object>} object array
   */
  static async mget (rowKeys, options) {
    options = _.defaults(options, {
      columns: this.columnNames
    })

    options.columns = this.conformColumns(options.columns)

    const gets = rowKeys.map(key => {
      const get = new HBase.Get(key)
      options.columns.forEach(col => {
        const [cf, q] = split(col)
        get.addColumn(cf, q)
      })
      return get
    })

    const results = await this.client.mgetAsync(this.tableName, gets)
    return results.map(result => this.formatAfterGet(result, options))
  }

  /**
   * put
   *
   * @param {string} rowKey the row key
   * @param {object} values if values is null and row key exists, the cell will be deleted
   */
  static async put (rowKey, values) {
    const columns = []
    const deleteColumns = []

    if (this.defaultFamily) {
      values = _.mapKeys(values, (v, k) => {
        if (k.indexOf(':') === -1) {
          return this.defaultFamily + ':' + k
        }
        return k
      })
    }

    _.forOwn(values, (value, col) => {
      if (value === undefined || value === null) {
        deleteColumns.push(col)
      } else {
        columns.push(col)
      }
    })

    if (columns.length > 0) {
      const put = new HBase.Put(rowKey)
      columns.forEach(col => {
        const value = values[col]
        const [cf, q] = split(col)
        put.add(cf, q, this.formatCellForPut(value, this.columnsObject[col].type))
      })
      await this.client.putAsync(this.tableName, put)
    }

    if (deleteColumns.length > 0) {
      const del = new HBase.Delete(rowKey)
      deleteColumns.forEach(col => {
        const [cf, q] = split(col)
        del.deleteColumns(cf, q)
      })
      await this.client.deleteAsync(this.tableName, del)
    }
  }

  /**
   * Delete a row
   *
   * @param {string} rowKey
   */
  static async destroy (rowKey) {
    const del = new HBase.Delete(rowKey)
    return this.client.deleteAsync(this.tableName, del)
  }

  /**
   * Scan
   *
   * @static
   * @param {object} options
   * @param {string} options.startRow the start row key, will be included in the result
   * @param {object} [options.stopRow] the stop row key, will be included in the result
   * @param {object} [options.limit] the number limit
   * @returns {Array<object>}
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

    const scanner = this.client.getScanner(this.tableName, startRow, stopRow)
    Bluebird.promisifyAll(scanner)

    while (count < limit && tmp) {
      tmp = await scanner.nextAsync()
      if (tmp && tmp.row) {
        results.push(tmp)
        count++
      } else {
        break
      }
    }

    return results.map(item => this.formatAfterGet(item))
  }

  static formatAfterGet (raw, options) {
    options = options || {}
    if (!raw) {
      return null
    }

    const rawObject = processRaw(raw)

    if (!rawObject) {
      return null
    }

    const df = this.defaultFamily
    const ret = {}

    this.columns.forEach(col => {
      if (options.columns && options.columns.indexOf(col.name) === -1) {
        return
      }

      let colName = col.name

      const value = this.formatCellForGet(rawObject[col.name], col.type)
      if (df && col.name.indexOf(df) === 0) {
        colName = colName.slice(df.length + 1)
      }
      ret[colName] = value
    })
    return ret
  }

  static formatCellForGet (value, type) {
    if (!type) {
      return value
    }
    if (typeof value === 'undefined') {
      return null
    }

    switch (type) {
      case 'string':
        return String(value)
      case 'number':
        return Number(value)
      case 'buffer':
        return value
      case 'date':
        return new Date(Number(value))
      default:
        return value
    }
  }

  static formatCellForPut (value, type) {
    if (typeof value === 'undefined' || value === null) {
      return value
    }

    switch (type) {
      case 'date':
        return String(value.getTime())
      case 'buffer':
        return value
      default:
        return String(value)
    }
  }

  static conformColumns (columns) {
    if (this.defaultFamily) {
      columns = columns.map(col => {
        if (col.indexOf(':') === -1) {
          return this.defaultFamily + ':' + col
        }
        return col
      })
      return columns
    }
    return columns
  }
}

function processRaw (raw) {
  if (!raw) {
    return null
  }

  // mget resutl this if key not exists
  if (raw.associatedCellCount === 0 && raw.exists === null) {
    return null
  }

  return raw.columns.reduce((acc, col) => {
    acc[`${col.family}:${col.qualifier}`] = col.value
    return acc
  }, {})
}

function split (col) {
  return col.split(':')
}

module.exports = Model
