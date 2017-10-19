const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helper')

const hb = helper.createHb({
  instanceMethods: {
    tomTest () {
      return this.data['cf1:string'] + '_tom'
    }
  }
})

test('Get data correctly by class method', async t => {
  const rowKey = uuid()
  const value = uuid()

  await hb.Test.put(rowKey, {
    'cf1:string': value
  })

  const record = await hb.Test.get(rowKey)

  t.is(record.tomTest(), value + '_tom')
})
