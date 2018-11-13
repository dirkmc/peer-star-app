'use strict'

const ORMap = require('delta-crdts')('ormap')
const hat = require('hat')

function crdtWithState (state) {
  const c = ORMap(hat())
  c.apply(state)
  return c
}

function firstSubsumesSecond (firstState, secondState) {
  const before = crdtWithState(firstState).value()
  const f = crdtWithState(firstState)
  f.apply(secondState)
  return eqMembership(before, f.value())
}

function eqMembership (obj1, obj2) {
  // Note: We only care about the peer ids in each membership, we don't care
  // about the peer addresses being different
  return eqSet(new Set(Object.keys(obj1)), new Set(Object.keys(obj2)))
}

function eqSet (s1, s2) {
  return s1.size === s2.size && new Set([...s1].concat([...s2])).size === s1.size
}

module.exports = {
  eqSet,
  eqMembership,
  crdtWithState,
  firstSubsumesSecond
}
