const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helpers')

const hb = helper.createHb()

test('Delete row correctly', async t => {
  const rowKey = uuid()
  const stringValue = uuid()

  const value = {
    'cf1:string': stringValue
  }

  await hb.Test.put(rowKey, value)

  const ret = await hb.Test.get(rowKey)
  t.truthy(ret)

  const result = await hb.Test.destroy(rowKey)
  t.true(typeof result === 'object' && result != null)
  t.true(result.processed)
  t.true(result.hasOwnProperty('result'))

  const ret2 = await hb.Test.get(rowKey)
  t.true(ret2 === null)
})
