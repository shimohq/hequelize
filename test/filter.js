const _ = require('lodash')
const Bluebird = require('bluebird')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helpers')

const hb = helper.createHb()

test('Get scan list correctly', async t => {
  const rowKeys = []
  const prefix = uuid()
  const count = 5

  for (let i = 0; i < count; i++) {
    rowKeys.push(`${prefix}-${_.padStart(i, 8, '0')}`)
  }

  await Bluebird.all(rowKeys.map((key, index) => {
    return hb.Test.put(key, {
      'cf1:string': 'tomtest' + index
    })
  }))

  const result1 = await hb.Test.scan({
    startRow: rowKeys[0],
    stopRow: rowKeys[rowKeys.length - 1],
    filters: {
      singleColumnValueFilter: {
        columnFamily: 'cf1',
        columnQualifier: 'string',
        compareOp: 'EQUAL',
        comparator: {
          regexStringComparator: {
            pattern: 'tomtest1',
            patternFlags: '',
            charset: 'UTF-8'
          }
        },
        filterIfMissing: true,
        latestVersionOnly: true
      }
    }
  })

  t.is(result1.length, 1)

  const result2 = await hb.Test.scan({
    startRow: rowKeys[0],
    stopRow: rowKeys[rowKeys.length - 1],
    filters: {
      singleColumnValueFilter: {
        columnFamily: 'cf1',
        columnQualifier: 'string',
        compareOp: 'NOT_EQUAL',
        comparator: {
          regexStringComparator: {
            pattern: 'tomtest1',
            patternFlags: '',
            charset: 'UTF-8'
          }
        },
        filterIfMissing: true,
        latestVersionOnly: true
      }
    }
  })

  t.is(result2.length, 4)
  t.true(result2.every(item => item['cf1:string'] !== 'tomtest1'))

  const result3 = await hb.Test.scan({
    startRow: rowKeys[0],
    stopRow: rowKeys[rowKeys.length - 1],
    filters: [
      {
        singleColumnValueFilter: {
          columnFamily: 'cf1',
          columnQualifier: 'string',
          compareOp: 'EQUAL',
          comparator: {
            regexStringComparator: {
              pattern: 'tomtest1',
              patternFlags: '',
              charset: 'UTF-8'
            }
          },
          filterIfMissing: true,
          latestVersionOnly: true
        }
      },
      {
        singleColumnValueFilter: {
          columnFamily: 'cf1',
          columnQualifier: 'string',
          compareOp: 'EQUAL',
          comparator: {
            regexStringComparator: {
              pattern: 'tomtest2',
              patternFlags: '',
              charset: 'UTF-8'
            }
          },
          filterIfMissing: true,
          latestVersionOnly: true
        }
      }
    ]
  })

  t.is(result3.length, 0)

  const result4 = await hb.Test.scan({
    startRow: rowKeys[0],
    stopRow: rowKeys[rowKeys.length - 1],
    filters: [
      {
        singleColumnValueFilter: {
          columnFamily: 'cf1',
          columnQualifier: 'string',
          compareOp: 'EQUAL',
          comparator: {
            regexStringComparator: {
              pattern: 'tomtest1',
              patternFlags: '',
              charset: 'UTF-8'
            }
          },
          filterIfMissing: true,
          latestVersionOnly: true
        }
      },
      {
        singleColumnValueFilter: {
          columnFamily: 'cf1',
          columnQualifier: 'string',
          compareOp: 'EQUAL',
          comparator: {
            regexStringComparator: {
              pattern: 'tomtest2',
              patternFlags: '',
              charset: 'UTF-8'
            }
          },
          filterIfMissing: true,
          latestVersionOnly: true
        }
      }
    ],
    filtersOptions: 'MUST_PASS_ONE'
  })

  t.is(result4.length, 2)
})
