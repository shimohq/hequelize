const _ = require('lodash')
const Bluebird = require('bluebird')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helpers')

const hb = helper.createHb()

test('Get scan list correctly', async t => {
  const rowKeys = []
  const prefix = uuid()
  const limit = 101
  const count = 111

  for (let i = 0; i < count; i++) {
    rowKeys.push(`${prefix}-${_.padStart(i, 8, '0')}`)
  }

  await Bluebird.all(rowKeys.map((key, index) => {
    return hb.Test.put(key, {
      'cf1:string': 'tomtest' + index
    })
  }))

  const results = await hb.Test.scan({
    startRow: rowKeys[0],
    stopRow: rowKeys[rowKeys.length - 1],
    limit
  })

  t.is(results.length, 101)

  rowKeys.slice(0, 101).forEach((rowKey, index) => {
    t.is(results[index]['cf1:string'], 'tomtest' + index)
  })

  const results2 = await hb.Test.scan({
    startRow: rowKeys[0],
    stopRow: rowKeys[rowKeys.length - 1],
    limit: 200
  })

  t.is(results2.length, count)
})
