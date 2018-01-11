const _ = require('lodash')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helpers')

const hb = helper.createHb()

test('Get scan list correctly', async t => {
  const rowKey = `${uuid()}-${_.padStart(1, 8, '0')}`
  const now = new Date()

  await hb.Test.put(rowKey, {
    'cf1:string': 'tomtest',
    'cf2:date': now
  })

  await hb.Test.checkAndPut(rowKey, 'cf2', 'date', null, { 'cf1:string': 'tomtest2' })
  const result1 = await hb.Test.get(rowKey)
  t.is(result1['cf1:string'], 'tomtest')

  await hb.Test.checkAndPut(rowKey, 'cf2', 'date', now, { 'cf1:string': 'tomtest2' })
  const result2 = await hb.Test.get(rowKey)
  t.is(result2['cf1:string'], 'tomtest2')
})

test('Returns processed result', async t => {
  const rowKey = `${uuid()}-${_.padStart(1, 8, '0')}`

  let results = await hb.Test.checkAndPut(
    rowKey,
    'cf1',
    'string',
    null,
    {
      'cf1:string': 'tomtest',
      'cf2:date': new Date()
    }
  )

  t.is(results.processed, true)
  t.is(results.result, null)

  results = await hb.Test.checkAndPut(rowKey, 'cf1', 'string', null, { 'cf1:string': 'tomtest2' })
  t.is(results.processed, false)

  results = await hb.Test.checkAndPut(
    rowKey,
    'cf1',
    'string',
    'tomtest',
    {
      'cf2:date': undefined
    }
  )
  t.is(results.processed, true)
})

test.serial('Throw error when not processed if throwOnFailure', async t => {
  const throwOnFailure = hb.Test.throwOnFailure
  hb.Test.throwOnFailure = true
  const rowKey = `${uuid()}-${_.padStart(1, 8, '0')}`

  await hb.Test.put(rowKey, {
    'cf1:string': 'tomtest',
    'cf2:date': new Date()
  })

  const error = await t.throws(
    hb.Test.checkAndPut(rowKey, 'cf2', 'date', null, { 'cf2:date': new Date() })
  )
  t.is(error.message, `hequelize failed to checkAndPut ${rowKey}`)

  hb.Test.throwOnFailure = throwOnFailure
})
