const _ = require('lodash')

function genRowKey (fileId, rev) {
  const salt = (1e10 + fileId).toString(36).split('').reverse().join('')
  const reversedRev = (1e10 - rev)
  return `${salt}_${fileId}_${reversedRev}_${rev}`
}

const examples = [
  [10000, 100],
  [10000, 101],
  [10000, 102],
  [10000, 103],
  [10001, 103]
]

examples.forEach(arr => {
  console.log(arr, genRowKey(arr[0], arr[1]))
})
