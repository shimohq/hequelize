const _ = require('lodash')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helper')

const hb = helper.createHb()

test('Get scan list correctly', async t => {
  const rowKey = `${uuid()}-${_.padStart(1, 8, '0')}`
  const now = new Date()

  await hb.Test.put(rowKey, {
    'cf1:string': 'tomtest',
    'cf2:date': now
  })

  const newDate = new Date(now)
  newDate.setDate(now.getDate() + 1)
  await hb.Test.checkAndPut(rowKey, 'cf2', 'date', null, { 'cf1:string': 'tomtest2' })
  const result1 = await hb.Test.get(rowKey)
  t.is(result1['cf1:string'], 'tomtest')

  await hb.Test.checkAndPut(rowKey, 'cf2', 'date', now, { 'cf1:string': 'tomtest2' })
  const result2 = await hb.Test.get(rowKey)
  t.is(result2['cf1:string'], 'tomtest2')
})
