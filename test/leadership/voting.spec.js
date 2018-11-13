/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(dirtyChai)

const codec = require('delta-crdts-msgpack-codec')
const CRDT = require('delta-crdts')
require('../../src/leadership/epoch-voters-crdt')
const { getLeader, vote, epochVotersIdentical } = require('../../src/leadership/voting')
const EpochVoters = CRDT('epochvoters')

describe('voting', () => {
  describe('getLeader', () => {
    it('gets null leadership for empty voters crdt', () => {
      const epochVoters = EpochVoters('id1')
      expect(getLeader(epochVoters)).to.equal(null)
    })

    it('gets correct leadership after one vote', () => {
      const epochVoters = EpochVoters('id1')
      epochVoters.vote('someid')
      expect(getLeader(epochVoters)).to.deep.equal({ leader: 'someid', votes: 1 })
    })

    it('gets null leadership after split vote', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      replica2.apply(replica1.vote('someid'))
      replica2.vote('someotherid')
      expect(getLeader(replica2)).to.equal(null)
    })

    it('gets null leadership after three way split vote', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      const replica3 = EpochVoters('id2')
      replica3.apply(replica1.vote('someid'))
      replica3.apply(replica2.vote('someotherid'))
      replica3.vote('athirdid')
      expect(getLeader(replica3)).to.equal(null)
    })

    it('gets correct leadership after deciding vote', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      const replica3 = EpochVoters('id3')
      replica3.apply(replica1.vote('someid'))
      replica3.apply(replica2.vote('someotherid'))
      replica3.vote('someotherid')
      expect(getLeader(replica3)).to.deep.equal({ leader: 'someotherid', votes: 2 })
    })

    it('gets correct leadership after advancing to new epoch', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      const replica3 = EpochVoters('id3')
      replica3.apply(replica1.vote('someid'))
      replica3.apply(replica2.vote('someotherid'))
      replica3.voteNewEpoch('someotherid')
      expect(getLeader(replica3)).to.deep.equal({ leader: 'someotherid', votes: 1 })
    })
  })

  describe('vote', () => {
    it('if there are no votes, votes for self', () => {
      const epochVoters = EpochVoters('id1')
      vote(epochVoters, 'id1')
      expect(epochVoters.value()).to.deep.equal([1, obj2Map({ id1: 'id1' })])
    })

    it('repeated votes make no difference', () => {
      const epochVoters = EpochVoters('id1')
      vote(epochVoters, 'id1')
      vote(epochVoters, 'id1')
      vote(epochVoters, 'id1')
      expect(epochVoters.value()).to.deep.equal([1, obj2Map({ id1: 'id1' })])
    })

    it('if there is one vote, votes for existing', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      vote(replica1, 'id1')
      replica2.apply(replica1.state())
      vote(replica2, 'id2')
      expect(replica2.value()).to.deep.equal([1, obj2Map({ id1: 'id1', id2: 'id1' })])
    })

    it('if theres a split vote, votes for highest id', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      const replica3 = EpochVoters('id3')
      vote(replica1, 'id1')
      vote(replica2, 'id2')
      replica3.apply(replica1.state())
      replica3.apply(replica2.state())
      vote(replica3, 'id3')
      expect(replica3.value()).to.deep.equal([1, obj2Map({
        id1: 'id1', id2: 'id2', id3: 'id2'
      })])
    })

    it('if there are several votes, votes for leader', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      const replica3 = EpochVoters('id3')
      const replica4 = EpochVoters('id4')
      vote(replica1, 'id1')
      vote(replica2, 'id2')
      replica3.apply(replica1.state())
      replica3.apply(replica2.state())
      vote(replica3, 'id3')
      replica4.apply(replica3.state())
      vote(replica4, 'id4')
      expect(replica4.value()).to.deep.equal([1, obj2Map({
        id1: 'id1', id2: 'id2', id3: 'id2', id4: 'id2'
      })])
    })
  })

  describe('epochVotersIdentical', () => {
    it('empty crdts are identical', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(true)
    })

    it('crdts with same single votes are identical', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      replica1.vote('id1')
      replica2.vote('id2')
      replica1.apply(replica2.state())
      replica2.apply(replica1.state())
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(true)
    })

    it('crdts with same multiple votes are identical', () => {
      const replica1 = EpochVoters('id1')
      const replica2 = EpochVoters('id2')
      const replica3 = EpochVoters('id3')
      replica1.vote('id1')
      replica2.vote('id2')
      replica3.vote('id1')
      replica1.apply(replica2.state())
      replica1.apply(replica3.state())
      replica2.apply(replica1.state())
      replica2.apply(replica3.state())
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(true)
    })

    it('crdt with one vote is different from empty crdt', () => {
      const replica1 = EpochVoters('id1')
      replica1.vote('id1')
      const replica2 = EpochVoters('id2')
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(false)
    })

    it('crdts with differing single voter and value are different', () => {
      const replica1 = EpochVoters('id1')
      replica1.vote('id1')
      const replica2 = EpochVoters('id2')
      replica2.vote('id2')
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(false)
    })

    it('crdts with same single voter but different value are different', () => {
      const replica1 = EpochVoters('id1')
      replica1.vote('id1')
      const replica2 = EpochVoters('id1')
      replica2.vote('id2')
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(false)
    })

    it('crdts with differing single voter but same value are different', () => {
      const replica1 = EpochVoters('id1')
      replica1.vote('id1')
      const replica2 = EpochVoters('id2')
      replica2.vote('id1')
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(false)
    })

    it('crdts with differing numbers of votes are different', () => {
      const replica1 = EpochVoters('id1')
      replica1.vote('id1')
      const replica2 = EpochVoters('id2')
      replica2.vote('id2')
      replica1.apply(replica2.state())
      expect(epochVotersIdentical(replica1.value(), replica2.value())).to.equal(false)
    })
  })
})

function obj2Map(obj) {
  return new Map(Object.entries(obj))
}
