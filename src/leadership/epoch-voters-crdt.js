/* eslint-env mocha */
'use strict'

const CRDT = require('delta-crdts')

const EpochVotersCRDT = {
  // The state is a two element array with
  // - the epoch in which the vote occurs
  // - the vote that each peer has made, eg { id1 => id2, id2 => id2, ... }
  initial: () => [1, new Map()],

  // To join two states
  // - if one has a higher epoch, just return that one
  // - if the epochs are equal, merge the voting maps
  join: (s1, s2) => {
    const epoch = Math.max(s1[0], s2[0])
    const res = [epoch]
    if (s1[0] > s2[0]) {
      res[1] = new Map(s1[1])
    } else if (s2[0] > s1[0]) {
      res[1] = new Map(s2[1])
    } else {
      res[1] = new Map([...s1[1], ...s2[1]])
    }
    return res
  },

  // The value is just a copy of the state
  value: (state) => [state[0], new Map(state[1])],

  mutators: {
    // Vote for a candidate
    vote (id, state, choice) {
      return [state[0], new Map([[id, choice]])]
    },
    // Increment the epoch and vote for a candidate in the new epoch
    voteNewEpoch (id, state, choice) {
      return [state[0] + 1, new Map([[id, choice]])]
    }
  }
}

CRDT.define('epochvoters', EpochVotersCRDT)
