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

    this.throwOnFailure = !!options.throwOnFailure
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
   *
   * @returns {object} { result: null, processed: true }
   */
  static async put (rowKey, values) {
    const putValues = this.formatPutValues(values)
    const { columns, deleteColumns } = putValues
    values = putValues.values

    let result = { result: null, processed: true }

    if (columns.length > 0) {
      const put = new HBase.Put(rowKey)
      columns.forEach(col => {
        const value = values[col]
        const [cf, q] = split(col)
        put.add(cf, q, this.formatCellForPut(value, this.columnsObject[col].type))
      })
      result = await this.client.putAsync(this.tableName, put)
    }

    if (deleteColumns.length > 0) {
      const del = new HBase.Delete(rowKey)
      deleteColumns.forEach(col => {
        const [cf, q] = split(col)
        del.deleteColumns(cf, q)
      })
      await this.client.deleteAsync(this.tableName, del)
    }

    return result
  }

  /**
   * checkAndPut
   *
   * @param {string} rowKey the row key
   * @param {string} family the column family to check
   * @param {string} qualifier the column qualifier to check
   * @param {*} value the cell value to compare
   * @param {object} putValues the values
   *
   * @returns {object} { result: null, processed: true }
   */
  static async checkAndPut (rowKey, family, qualifier, value, putValues) {
    const results = this.formatPutValues(putValues)
    const { columns, deleteColumns } = results
    putValues = results.values

    const col = this.columnsObject[`${family}:${qualifier}`]
    value = this.formatCellForPut(value, col.type)

    let putResult = { result: null, processed: true }

    if (columns.length > 0) {
      const put = new HBase.Put(rowKey)
      columns.forEach(col => {
        const value = putValues[col]
        const [cf, q] = split(col)
        put.add(cf, q, this.formatCellForPut(value, this.columnsObject[col].type))
      })

      putResult = await this.client.checkAndPutAsync(
        this.tableName, rowKey, family, qualifier, value, put
      )
    }

    if (deleteColumns.length > 0) {
      await this.checkAndDelete(rowKey, family, qualifier, value, deleteColumns)
    }

    if (this.throwOnFailure && !putResult.processed) {
      throw new Error(`hequelize failed to checkAndPut ${rowKey}`)
    }

    return putResult
  }

  /**
   * checkAndDelete
   *
   * @param {string} rowKey the row key
   * @param {string} family the column family to check
   * @param {string} qualifier the column qualifier to check
   * @param {*} value the cell value to compare
   * @param {string[]} qualifiers the qualifiers to be deleted
   *
   * @returns {object} { result: null, processed: true }
   */
  static async checkAndDelete (rowKey, family, qualifier, value, qualifiers) {
    qualifiers = [].concat(qualifiers).filter(q => typeof q === 'string')

    if (qualifiers.length < 1) {
      throw new Error(`qualifier list is empty`)
    }

    const del = new HBase.Delete(rowKey)

    this.conformColumns(qualifiers).forEach(col => {
      const [cf, q] = split(col)
      del.deleteColumns(cf, q)
    })

    const col = this.columnsObject[`${family}:${qualifier}`]
    value = this.formatCellForPut(value, col.type)

    const result = this.client.checkAndDeleteAsync(
      this.tableName, rowKey, family, qualifier, value, del
    )

    if (this.throwOnFailure && !result.processed) {
      throw new Error(`hequelize failed to checkAndDelete ${rowKey}`)
    }

    return result
  }

  /**
   * Delete a row
   *
   * @param {string} rowKey
   *
   * @returns {object} whether processed, e.g. { result: null, processed: true }
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
   * @param {object|array.<object>} [options.filters] the filter(s)
   * @param {string} [options.filtersOptions] the filter options, default is 'MUST_PASS_ALL'
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

    setFilters(scanner, options.filters, options.filtersOptions)

    while (count < limit && tmp) {
      try {
        tmp = await scanner.nextAsync()
      } catch (e) {
        // Workaround for 'org.apache.hadoop.hbase.UnknownScannerException: Unknown scanner'.
        // https://github.com/falsecz/hbase-rpc-client/blob/7293c97d6cbf6c7fa7d8655e5d7ea78759135d05/src/scan.coffee#L84
        if ((e.stackTrace || '').includes('Unknown scanner')) {
          tmp = null
        } else {
          throw e
        }
      }

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

  static formatPutValues (values) {
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

    return { columns, deleteColumns, values }
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

function setFilters (scanner, filters, filtersOptions) {
  if (filters == null || typeof filters !== 'object') {
    return
  }

  if (!Array.isArray(filters)) {
    filters = [filters]
  }

  if (filters.length < 1) {
    return
  }

  const filterList = new HBase.FilterList(filtersOptions || 'MUST_PASS_ALL')

  filters.forEach(filter => filterList.addFilter(filter))

  scanner.setFilter(filterList)
}

module.exports = Model
