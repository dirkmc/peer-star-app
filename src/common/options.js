'use strict'

function merge(...args) {
  let res
  for (const arg of args) {
    const clone = arg ? Object.create(
      Object.getPrototypeOf(arg), 
      Object.getOwnPropertyDescriptors(arg) 
    ) : {}
    if (res) {
      for (const k of Object.getOwnPropertyNames(clone)) {
        const desc = Object.getOwnPropertyDescriptor(clone, k)
        Object.defineProperty(res, k, desc)
      }
    } else {
      res = clone
    }
  }
  return res
}

module.exports = {
  merge
}
