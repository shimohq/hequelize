const _ = require('lodash')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helpers')

const hb = helper.createHb()

test('Get data correctly', async t => {
  const rowKey = uuid()
  const stringValue = uuid()
  const dateValue = new Date()

  const value = {
    'cf1:string': stringValue,
    'cf2:date': dateValue
  }

  await hb.Test.put(rowKey, value)

  const ret = await hb.Test.get(rowKey)

  t.true(_.isDate(ret['cf2:date']))
  t.deepEqual(ret, {
    'cf1:string': stringValue,
    'cf2:date': dateValue,
    'cf2:buffer': null,
    'cf2:number': null
  })
})

test('Replace a non-null value with null successfully', async t => {
  const rowKey = uuid()
  const stringValue = uuid()

  await hb.Test.put(rowKey, {
    'cf1:string': stringValue,
    'cf2:number': 123
  })

  await hb.Test.put(rowKey, {
    'cf1:string': null
  })

  const ret = await hb.Test.get(rowKey)
  t.deepEqual(ret, {
    'cf1:string': null,
    'cf2:date': null,
    'cf2:buffer': null,
    'cf2:number': 123
  })
})

test('put returns process result', async t => {
  const rowKey = uuid()
  const stringValue = uuid()

  let result = await hb.Test.put(rowKey, {
    'cf1:string': stringValue,
    'cf2:number': 123
  })
  t.is(result.processed, true)
  t.is(result.result, null)

  result = await hb.Test.put(rowKey, {
    'cf1:string': stringValue,
    'cf2:number': undefined
  })
  t.is(result.processed, true)
})
