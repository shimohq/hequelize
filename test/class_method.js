const _ = require('lodash')
const test = require('ava')
const uuid = require('uuid/v4')
const helper = require('./helper')

function reverse (str) {
  return str.split('').reverse().join('')
}

const hb = helper.createHb({
  classMethods: {
    fileIdToKey (fileId) {
      return reverse((1e10 - fileId).toString(36))
    },

    keyToFileId (rowKey) {
      return Math.abs(parseInt(reverse(rowKey), 36) - 1e10)
    },

    async tomGet (fileId) {
      const rowKey = this.fileIdToKey(fileId)
      return this.get(rowKey)
    }
  }
})

test('Get data correctly by class method', async t => {
  const fileId = parseInt(Math.random() * 1e7)
  const rowKey = hb.Test.fileIdToKey(fileId)

  const value = uuid()
  await hb.Test.put(rowKey, {
    'cf1:string': value + fileId
  })

  const record = await hb.Test.tomGet(fileId)

  t.is(record['cf1:string'], value + fileId)
})
