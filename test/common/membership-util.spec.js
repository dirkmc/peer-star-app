/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const membershipUtil = require('../../src/common/membership-util')
const CRDT = require('delta-crdts')
const ORMap = CRDT('ormap')

describe('membership util', () => {
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
        expect(membershipUtil.firstSubsumesSecond(replica2.state(), replica1.state())).to.equal(true)
      })

      it('returns false when first does not have all seconds changes', () => {
        expect(membershipUtil.firstSubsumesSecond(replica1.state(), replica2.state())).to.equal(false)
      })
    })

    describe('adds and removes', () => {
      const replica1 = ORMap('id1')
      replica1.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
      replica1.applySub('peer2', 'mvreg', 'write', ['addr2.a'])
      replica1.remove('peer1')

      const replica2 = ORMap('id2')
      replica2.applySub('peer2', 'mvreg', 'write', ['addr2.a'])

      const replica3 = ORMap('id3')
      replica3.applySub('peer1', 'mvreg', 'write', ['addr1.a'])
      replica3.applySub('peer2', 'mvreg', 'write', ['addr2.a'])

      it('returns true because peer1 + peer2 - peer1 = peer2', () => {
        expect(membershipUtil.firstSubsumesSecond(replica1.state(), replica2.state())).to.equal(true)
      })

      it('returns true because peer2 = peer1 + peer2 - peer1', () => {
        expect(membershipUtil.firstSubsumesSecond(replica2.state(), replica1.state())).to.equal(true)
      })

      it('returns true because peer2 + peer1 > peer1 (peer1 + peer2 - peer1)', () => {
        expect(membershipUtil.firstSubsumesSecond(replica3.state(), replica1.state())).to.equal(true)
      })

      it('returns false because peer1 + peer2 - peer1 = peer1 which is < peer2 + peer1', () => {
        expect(membershipUtil.firstSubsumesSecond(replica1.state(), replica3.state())).to.equal(false)
      })
    })
  })
})
