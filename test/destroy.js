const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helper')

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

  await hb.Test.destroy(rowKey)
  const ret2 = await hb.Test.get(rowKey)
  t.true(ret2 === null)
})
