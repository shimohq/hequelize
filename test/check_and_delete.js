const _ = require('lodash')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helpers')

const hb = helper.createHb()

test('Delete qualifiers if value matched', async t => {
  const rowKey = `${uuid()}-${_.padStart(1, 8, '0')}`
  const now = new Date()

  await hb.Test.put(rowKey, {
    'cf1:string': 'tomtest',
    'cf2:date': now
  })

  const ret = await hb.Test.checkAndDelete(rowKey, 'cf2', 'date', now, ['cf2:date'])
  t.true(typeof ret === 'object' && ret != null)
  t.is(ret.result, null)
  t.is(ret.processed, true)

  const result1 = await hb.Test.get(rowKey)
  t.is(result1['cf1:string'], 'tomtest')
  t.not(result1['cf2:date'], now)

  await hb.Test.put(rowKey, {
    'cf1:string': 'tomtest',
    'cf2:date': now
  })
  await hb.Test.checkAndDelete(rowKey, 'cf2', 'date', null, ['cf2:date'])
  const result2 = await hb.Test.get(rowKey)
  t.is(result2['cf1:string'], 'tomtest')
  t.is(result2['cf2:date'].getTime(), now.getTime())
})

test('Throw error when qualifer list is emptry', async t => {
  const error = await t.throws(
    hb.Test.checkAndDelete('', 'cf2', 'date', null, [])
  )
  t.is(error.message, 'qualifier list is empty')
})

test.serial('Throw error when not processed if throwOnFailure', async t => {
  const throwOnFailure = hb.Test.throwOnFailure
  hb.Test.throwOnFailure = true
  const rowKey = `${uuid()}-${_.padStart(1, 8, '0')}`

  const error = await t.throws(
    hb.Test.checkAndDelete(rowKey, 'cf2', 'date', null, ['cf2:date'])
  )
  t.is(error.message, `hequelize failed to checkAndDelete ${rowKey}`)

  hb.Test.throwOnFailure = throwOnFailure
})
