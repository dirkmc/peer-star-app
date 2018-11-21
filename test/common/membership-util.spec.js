/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const { eqSet, crdtWithState, eqMembership, firstSubsumesSecond } = require('../../src/common/membership-util')
const CRDT = require('delta-crdts')
const ORMap = CRDT('ormap')

describe('membership util', () => {
  describe('eqSet', () => {
    it('returns true when sets are empty', () => {
      expect(eqSet(new Set(), new Set())).to.equal(true)
    })
    it('returns true when first set has same members as second set', () => {
      expect(eqSet(new Set([1, 2, 3]), new Set([1, 2, 3]))).to.equal(true)
    })
    it('returns true when sets are in different order', () => {
      expect(eqSet(new Set([1, 2, 3]), new Set([3, 2, 1]))).to.equal(true)
    })

    it('returns false when sets have same order but different members', () => {
      expect(eqSet(new Set([1, 2, 3]), new Set([1, 2, 4]))).to.equal(false)
    })
    it('returns false when first set is empty and second is not', () => {
      expect(eqSet(new Set(), new Set([1]))).to.equal(false)
    })
    it('returns false when second set is empty and first is not', () => {
      expect(eqSet(new Set([1]), new Set())).to.equal(false)
    })
    it('returns false when sets have same members but first has less', () => {
      expect(eqSet(new Set([1, 2]), new Set([1, 2, 3]))).to.equal(false)
    })
    it('returns false when sets have same members but second has less', () => {
      expect(eqSet(new Set([1, 2, 3]), new Set([1, 2]))).to.equal(false)
    })
    it('returns false when sets have same members but of different types', () => {
      expect(eqSet(new Set([1, 2]), new Set(['1', '2']))).to.equal(false)
    })
  })

  describe('eqMembership', () => {
    it('returns true when peers are same, even if addresses are not', () => {
      const replica1 = ORMap('id1')
      replica1.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
      replica1.applySub('peer2', 'mvreg', 'write', ['addr2.a'])
      const replica2 = ORMap('id2')
      replica2.applySub('peer1', 'mvreg', 'write', ['addr3.a'])
      replica2.applySub('peer2', 'mvreg', 'write', ['addr4.a'])
      expect(eqMembership(replica1.value(), replica2.value())).to.equal(true)
    })
  })

  describe('crdtWithState', () => {
    it('returns a copy of the crdt with the same state', () => {
      const replica1 = ORMap('id1')
      replica1.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
      replica1.applySub('peer2', 'mvreg', 'write', ['addr2.a'])
      const copy = crdtWithState(replica1.state())
      expect(copy.value()).to.deep.equal({
        peer1: new Set([['addr1.a']]),
        peer2: new Set([['addr2.a']])
      })
    })
  })

  describe('firstSubsumesSecond', () => {
    describe('adds', () => {
      let replica1, replica2
      before(() => {
        replica1 = ORMap('id1')
        replica1.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
        replica1.applySub('peer2', 'mvreg', 'write', ['addr2.a'])

        replica2 = ORMap('id2')
        replica2.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
        replica2.applySub('peer2', 'mvreg', 'write', ['addr2.a'])
        replica2.applySub('peer3', 'mvreg', 'write', ['addr3.a'])
      })

      it('returns true when first has all seconds changes', () => {
        expect(firstSubsumesSecond(replica2.state(), replica1.state())).to.equal(true)
      })

      it('returns false when first does not have all seconds changes', () => {
        expect(firstSubsumesSecond(replica1.state(), replica2.state())).to.equal(false)
      })
    })

    describe('different addresses', () => {
      let replica1, replica2
      before(() => {
        replica1 = ORMap('id1')
        replica1.applySub('peer1', 'mvreg', 'write', ['addr1.a'])

        replica2 = ORMap('id2')
        replica2.applySub('peer1', 'mvreg', 'write', ['other.a'])
      })

      it('returns true when replicas have same peers even if peer addresses are different', () => {
        expect(firstSubsumesSecond(replica2.state(), replica1.state())).to.equal(true)
      })
    })

    describe('adds and removes', () => {
      let replica1, replica2, replica3
      before(() => {
        replica1 = ORMap('id1')
        replica1.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
        replica1.applySub('peer2', 'mvreg', 'write', ['addr2.a'])
        replica1.remove('peer1')

        replica2 = ORMap('id2')
        replica2.applySub('peer2', 'mvreg', 'write', ['addr2.a'])

        replica3 = ORMap('id3')
        replica3.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
        replica3.applySub('peer2', 'mvreg', 'write', ['addr2.a'])
      })

      it('returns true because peer1 + peer2 - peer1 = peer2', () => {
        expect(firstSubsumesSecond(replica1.state(), replica2.state())).to.equal(true)
      })

      it('returns true because peer2 = peer1 + peer2 - peer1', () => {
        expect(firstSubsumesSecond(replica2.state(), replica1.state())).to.equal(true)
      })

      it('returns true because peer2 + peer1 > peer1 (peer1 + peer2 - peer1)', () => {
        expect(firstSubsumesSecond(replica3.state(), replica1.state())).to.equal(true)
      })

      it('returns false because peer1 + peer2 - peer1 = peer1 which is < peer2 + peer1', () => {
        expect(firstSubsumesSecond(replica1.state(), replica3.state())).to.equal(false)
      })
    })
  })
})
