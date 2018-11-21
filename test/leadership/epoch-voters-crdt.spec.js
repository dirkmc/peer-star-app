/* eslint-env mocha */
'use strict'

const chai = require('chai')
const dirtyChai = require('dirty-chai')
const expect = chai.expect
chai.use(dirtyChai)

const codec = require('delta-crdts-msgpack-codec')
const CRDT = require('delta-crdts')
require('../../src/leadership/epoch-voters-crdt')
const Voting = require('../../src/leadership/voting')

describe('epoch voters', () => {
  describe('local', () => {
    let EpochVoters
    let epochVoters
    it('type can be created', () => {
      EpochVoters = CRDT('epochvoters')
    })

    it('can be instantiated', () => {
      epochVoters = EpochVoters('id1')
    })

    it('starts empty at epoch 1', () => {
      expect(epochVoters.value()).to.deep.equal([1, new Map()])
    })

    it('can vote', () => {
      epochVoters.vote('someid')
    })

    it('and the vote is recorded', () => {
      expect(epochVoters.value()).to.deep.equal([1, obj2Map({'id1': 'someid'})])
      expect(Voting.getLeader(epochVoters)).to.deep.equal({ leader: 'someid', votes: 1 })
    })

    it('can advance the epoch and vote', () => {
      epochVoters.voteNewEpoch('someotherid')
    })

    it('and the vote is recorded and epoch incremented', () => {
      expect(epochVoters.value()).to.deep.equal([2, obj2Map({'id1': 'someotherid'})])
      expect(Voting.getLeader(epochVoters)).to.deep.equal({ leader: 'someotherid', votes: 1 })
    })
  })

  describe('convergence', () => {
    let EpochVoters = CRDT('epochvoters')

    let replica1, replica2, replica3
    let deltas = [[], [], []]
    before(() => {
      replica1 = EpochVoters('id1')
      replica2 = EpochVoters('id2')
      replica3 = EpochVoters('id3')
    })

    it('can vote', () => {
      deltas[0].push(replica1.vote('apples'))
      deltas[1].push(replica2.vote('oranges'))
      deltas[2].push(replica3.vote('apples'))
    })

    it('changes from others can be joined to the first', () => {
      deltas[1].forEach((delta) => replica1.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica1.apply(transmit(delta)))
    })

    it('changes from others can be joined to the second', () => {
      deltas[0].forEach((delta) => replica2.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica2.apply(transmit(delta)))
    })

    it('changes from others can be joined to the third', () => {
      deltas[0].forEach((delta) => replica3.apply(transmit(delta)))
      deltas[1].forEach((delta) => replica3.apply(transmit(delta)))
    })

    const exp = {
      votes: {
        'id1': 'apples',
        'id2': 'oranges',
        'id3': 'apples'
      },
      leadership: {
        leader: 'apples',
        votes: 2
      }
    }

    it('the first converges', () => {
      expect(replica1.value()).to.deep.equal([1, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica1)).to.deep.equal(exp.leadership)
    })

    it('and the second also converges', () => {
      expect(replica2.value()).to.deep.equal([1, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica2)).to.deep.equal(exp.leadership)
    })

    it('and the third also converges', () => {
      expect(replica3.value()).to.deep.equal([1, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica3)).to.deep.equal(exp.leadership)
    })
  })

  describe('no leader', () => {
    let EpochVoters = CRDT('epochvoters')

    let replica1, replica2, replica3
    let deltas = [[], [], []]
    before(() => {
      replica1 = EpochVoters('id1')
      replica2 = EpochVoters('id2')
      replica3 = EpochVoters('id3')
    })

    it('first two can vote', () => {
      deltas[0].push(replica1.vote('apples'))
      deltas[1].push(replica2.vote('oranges'))
    })

    it('changes from second can be joined to the first', () => {
      deltas[1].forEach((delta) => replica1.apply(transmit(delta)))
    })

    it('changes from first can be joined to the second', () => {
      deltas[0].forEach((delta) => replica2.apply(transmit(delta)))
    })

    it('there is no leader in the first', () => {
      expect(Voting.getLeader(replica1)).to.be.null()
    })

    it('there is no leader in the second', () => {
      expect(Voting.getLeader(replica2)).to.be.null()
    })

    it('third vote is tie-breaker', () => {
      deltas[2].push(replica3.vote('apples'))
      deltas[2].forEach((delta) => replica1.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica2.apply(transmit(delta)))
      expect(Voting.getLeader(replica1)).to.deep.equal({ leader: 'apples', votes: 2 })
      expect(Voting.getLeader(replica2)).to.deep.equal({ leader: 'apples', votes: 2 })
    })
  })

  describe('epoch change', () => {
    let EpochVoters = CRDT('epochvoters')

    let replica1, replica2, replica3
    before(() => {
      replica1 = EpochVoters('id1')
      replica2 = EpochVoters('id2')
      replica3 = EpochVoters('id3')
    })

    it('after join replicas converge', () => {
      const deltas = [[], [], []]
      deltas[0].push(replica1.vote('apples'))
      deltas[1].push(replica2.vote('oranges'))
      deltas[2].push(replica3.vote('oranges'))

      deltas[1].forEach((delta) => replica1.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica1.apply(transmit(delta)))
      deltas[0].forEach((delta) => replica2.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica2.apply(transmit(delta)))
      deltas[0].forEach((delta) => replica3.apply(transmit(delta)))
      deltas[1].forEach((delta) => replica3.apply(transmit(delta)))

      const exp = {
        votes: {
          'id1': 'apples',
          'id2': 'oranges',
          'id3': 'oranges'
        },
        leadership: {
          leader: 'oranges',
          votes: 2
        }
      }

      expect(replica1.value()).to.deep.equal([1, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica1)).to.deep.equal(exp.leadership)
      expect(replica2.value()).to.deep.equal([1, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica2)).to.deep.equal(exp.leadership)
      expect(replica3.value()).to.deep.equal([1, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica3)).to.deep.equal(exp.leadership)
    })

    it('when one replica moves to next epoch, after join replicas ignore previous epoch', () => {
      const deltas = [[], [], []]
      deltas[0].push(replica1.voteNewEpoch('oranges'))

      deltas[0].forEach((delta) => replica2.apply(transmit(delta)))
      deltas[0].forEach((delta) => replica3.apply(transmit(delta)))

      const exp = {
        votes: {
          'id1': 'oranges'
        },
        leadership: {
          leader: 'oranges',
          votes: 1
        }
      }

      expect(replica1.value()).to.deep.equal([2, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica1)).to.deep.equal(exp.leadership)
      expect(replica2.value()).to.deep.equal([2, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica2)).to.deep.equal(exp.leadership)
      expect(replica3.value()).to.deep.equal([2, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica3)).to.deep.equal(exp.leadership)
    })

    it('subsequent votes occur in the new epoch', () => {
      const deltas = [[], [], []]
      deltas[1].push(replica2.vote('apples'))
      deltas[2].push(replica3.vote('apples'))

      deltas[1].forEach((delta) => replica1.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica1.apply(transmit(delta)))
      deltas[1].forEach((delta) => replica3.apply(transmit(delta)))
      deltas[2].forEach((delta) => replica2.apply(transmit(delta)))

      const exp = {
        votes: {
          'id1': 'oranges',
          'id2': 'apples',
          'id3': 'apples'
        },
        leadership: {
          leader: 'apples',
          votes: 2
        }
      }

      expect(replica1.value()).to.deep.equal([2, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica1)).to.deep.equal(exp.leadership)
      expect(replica2.value()).to.deep.equal([2, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica2)).to.deep.equal(exp.leadership)
      expect(replica3.value()).to.deep.equal([2, obj2Map(exp.votes)])
      expect(Voting.getLeader(replica3)).to.deep.equal(exp.leadership)
    })
  })
})

function obj2Map(obj) {
  return new Map(Object.entries(obj))
}

function transmit (delta) {
  return codec.decode(codec.encode(delta))
}
