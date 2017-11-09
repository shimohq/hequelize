// const _ = require('lodash')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helper')

const hb = helper.createHb()

test('Get data correctly', async t => {
  const key1 = uuid()
  const key2 = uuid()

  const key1String = 'key1string'
  const key2String = 'key2string'

  await hb.Test.put(key1, {
    'cf1:string': key1String,
    'cf2:number': 100
  })

  await hb.Test.put(key2, {
    'cf1:string': key2String,
    'cf2:number': 200
  })

  const ret = await hb.Test.mget([key1, key2], { columns: ['cf1:string'] })

  t.true(ret.length === 2)
  t.deepEqual(ret[0], {
    'cf1:string': 'key1string'
  })
  t.deepEqual(ret[1], {
    'cf1:string': 'key2string'
  })

  const ret2 = await hb.Test.mget([key1, key2 + 'not-existed'])
  t.true(ret2[1] === null)
  t.deepEqual(ret2[0], {
    'cf1:string': 'key1string',
    'cf2:number': 100,
    'cf2:buffer': null,
    'cf2:date': null
  })
})
