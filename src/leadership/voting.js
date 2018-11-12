'use strict'

const membershipUtil = require('../common/membership-util')

module.exports = {
  getLeader (crdt) {
    const votes = crdt.state()[1]

    // Count how many votes each candidate got
    const countByChoice = [...votes].reduce((acc, [voter, choice]) => {
      acc.set(choice, (acc.get(choice) || 0) + 1)
      return acc
    }, new Map())

    // Sort them such that the highest number of votes is first
    const sorted = [...countByChoice].sort((a, b) => b[1] - a[1])

    // If there is at least one vote, and there wasn't a tie
    // record the winner
    if (sorted.length && sorted[0][1] !== (sorted[1] || [])[1]) {
      return {
        leader: sorted[0][0],
        votes: sorted[0][1]
      }
    }
    return null
  },

  vote (crdt, selfId) {
    const votesSoFar = crdt.state()[1]

    // If there are no votes, vote for self
    if (!votesSoFar.size) {
      crdt.vote(selfId)
      return
    }

    // Already voted
    if (votesSoFar.has(selfId)) return

    // Otherwise vote for the leader
    const countByChoice = [...votesSoFar].reduce((acc, [voter, choice]) => {
      acc.set(choice, (acc.get(choice) || 0) + 1)
      return acc
    }, new Map())

    const sorted = [...countByChoice].sort((a, b) => {
      // Sort by vote count, then by ID (as a tie breaker)
      let cmp = b[1] - a[1]
      if (cmp === 0) {
        if (b[0] > a[0]) {
          cmp = 1
        } else if (b[0] < a[0]) {
          cmp = -1
        }
      }
      return cmp
    })
    crdt.vote(sorted[0][0])
  },

  epochVotersIdentical (ev1, ev2) {
    // Check epoch matches
    if (ev1[0] !== ev2[0]) {
      return false
    }

    // Check voters match
    const m1 = ev1[1]
    const m2 = ev2[1]
    if (!membershipUtil.eqSet(new Set(m1.keys()), new Set(m2.keys()))) {
      return false
    }

    // Check votes match
    for (const k of m1.keys()) {
      if (m1.get(k) !== m2.get(k)) {
        return false
      }
    }
    return true
  }
}
