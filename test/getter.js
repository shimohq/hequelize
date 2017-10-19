const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helper')

const hb = helper.createHb({
  columns: [{
    name: 'cf1:string',
    type: 'string',
    get (value) {
      return value + '_getter'
    }
  }]
})

test('Get data correctly by column getter', async t => {
  const rowKey = uuid()
  const value = uuid()

  await hb.Test.put(rowKey, {
    'cf1:string': value
  })

  const record = await hb.Test.get(rowKey)

  t.is(record['cf1:string'], value + '_getter')
})
