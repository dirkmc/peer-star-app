/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const FakePeerInfo = require('../utils/fake-peer-info')
const randomPeerId = require('../utils/random-peer-id').buffer
const EventEmitter = require('events')
const Leadership = require('../../src/leadership')
const LeadershipState = Leadership.LeadershipState
const ORMap = require('delta-crdts')('ormap')
const EpochVoters = require('delta-crdts')('epochvoters')

function randomB58String () {
  return new FakePeerInfo(randomPeerId()).id.toB58String()
}

const mock = {
  gossipFrequencyHeuristic () {
    const gfh = new EventEmitter()
    gfh.start = () => {}
    gfh.stop = () => {}
    return gfh
  }
}

function generateGossipMessage (voteForPeerId) {
  const peerId = randomB58String()
  const membership = ORMap(peerId)
  membership.applySub(peerId, 'mvreg', 'write', [])
  const message = ['collab', membership.state(), 'rga']
  const epochVoters = EpochVoters(peerId)
  epochVoters.vote(voteForPeerId || peerId)

  return {
    peerId,
    epochVoters,
    message
  }
}

function mergeEpochVoters (epochVoters1, epochVoters2) {
  const mergedEpochVoters = EpochVoters('tmp')
  mergedEpochVoters.apply(epochVoters1.state())
  mergedEpochVoters.apply(epochVoters2.state())
  return mergedEpochVoters
}

function expectDiscoveryState (leadership) {
  expect(leadership.getLeader()).to.equal(undefined)
  expect(leadership.getState()).to.equal(LeadershipState.Discovery)
  expect(leadership.needsUrgentBroadcast()).to.equal(false)
  expect(leadership.getGossipMessage(false)).to.deep.equal({ leader: null })
  expect(leadership.getGossipMessage(true)).to.deep.equal({ leader: null })
}

function expectVotingState (leadership, epochVotersState) {
  expect(leadership.getLeader()).to.equal(null)
  expect(leadership.getState()).to.equal(LeadershipState.Voting)
  expect(leadership.needsUrgentBroadcast()).to.equal(true)
  expect(leadership.getGossipMessage(false)).to.deep.equal({ leader: null })

  // console.log('=====')
  // console.log(leadership.getGossipMessage(true).epochVoters)
  // console.log('-----')
  // console.log(epochVotersState)
  // console.log('=====')

  expect(leadership.getGossipMessage(true)).to.deep.equal({ leader: null, epochVoters: epochVotersState })
}

function expectVotingStateNewEpochNumber (leadership, newEpochNumber) {
  const newState = [
    newEpochNumber,
    new Map([[leadership._peerId, leadership._peerId]])
  ]
  expectVotingState(leadership, newState)
}

function expectKnownState (leadership, peerId) {
  expect(leadership.getLeader()).to.equal(peerId)
  expect(leadership.getState()).to.equal(LeadershipState.Known)
  expect(leadership.needsUrgentBroadcast()).to.equal(false)
  expect(leadership.getGossipMessage(false)).to.deep.equal({ leader: peerId })
  expect(leadership.getGossipMessage(true)).to.deep.equal({ leader: peerId })
}

describe('leadership', function () {
  const leaderships = []
  function createLeadership (options, gossipFrequencyHeuristic) {
    const gfh = gossipFrequencyHeuristic || mock.gossipFrequencyHeuristic()
    const membership = new EventEmitter()
    membership.peerCount = () => 1
    const leadership = new Leadership(membership, gfh, options)
    leaderships.push(leadership)
    return leadership
  }

  async function getStartedLeadershipWithSelfAsLeader () {
    const gfh = mock.gossipFrequencyHeuristic()
    const leadership = createLeadership({
      leadershipElectionGossipNowMaxCount: 1
    }, gfh)
    const peerId = randomB58String()
    leadership.start(peerId)
    gfh.emit('gossip now')
    await new Promise(resolve => setTimeout(resolve))

    const epochVoters = EpochVoters(peerId)
    epochVoters.vote(peerId)

    const localMembership = ORMap(peerId)
    localMembership.applySub(peerId, 'mvreg', 'write', [])

    return {
      peerId,
      epochVoters,
      localMembership,
      leadership
    }
  }

  after(() => leaderships.forEach(l => l.stop()))

  describe('Bad message handling', function () {
    it('Should ignore bad messages', async () => {
      const { leadership, localMembership } = await getStartedLeadershipWithSelfAsLeader()

      let succeeded = true
      try {
        // no params
        leadership.deliverGossipMessage()
        // undefined params
        leadership.deliverGossipMessage(undefined, undefined, undefined)
        // undefined message array elements
        leadership.deliverGossipMessage(undefined, [undefined, undefined], undefined)
        // undefined leader
        leadership.deliverGossipMessage(localMembership.state(), '', { leader: undefined })
        // undefined epochVoters
        leadership.deliverGossipMessage(localMembership.state(), '', { epochVoters: undefined })
      } catch (e) {
        succeeded = false
      }
      expect(succeeded).to.equal(true)
    })
  })

  describe('Discovery state', function () {
    it('before starting should be in Discovery state', async () => {
      const leadership = createLeadership()
      expectDiscoveryState(leadership)
    })

    it('after starting should still be in Discovery state', async () => {
      const leadership = createLeadership()
      const peerId = randomB58String()
      leadership.start(peerId)
      expectDiscoveryState(leadership)
    })

    it('should elect self as leader if no gossip messages are received', async () => {
      const gfh = mock.gossipFrequencyHeuristic()
      const leadership = createLeadership({
        leadershipElectionGossipNowMaxCount: 3
      }, gfh)
      const peerId = randomB58String()
      leadership.start(peerId)
      gfh.emit('gossip now')
      gfh.emit('gossip now')
      gfh.emit('gossip now')
      await new Promise(resolve => setTimeout(resolve))

      expectKnownState(leadership, peerId)
    })

    it('from Discovery state, should accept leadership in incoming gossip message', async () => {
      const leadership = createLeadership()
      const peerId = randomB58String()
      leadership.start(peerId)

      const localMembership = ORMap(peerId)
      localMembership.applySub(peerId, 'mvreg', 'write', [])
      const remotePeerId = randomB58String()
      leadership.deliverGossipMessage(localMembership.state(), 'membership hash', { leader: remotePeerId })

      expectKnownState(leadership, remotePeerId)
    })

    it('from Discovery state, should accept leadership and apply votes in incoming gossip message', async () => {
      const leadership = createLeadership()
      const peerId = randomB58String()
      leadership.start(peerId)

      const localMembership = ORMap(peerId)
      localMembership.applySub(peerId, 'mvreg', 'write', [])

      const remote = generateGossipMessage()
      leadership.deliverGossipMessage(localMembership.state(), remote.message, {
        leader: remote.peerId,
        epochVoters: remote.epochVoters.state()
      })

      expectKnownState(leadership, remote.peerId)
    })
  })

  describe('Known state', function () {
    it('From Known state should not change state if gossip summary message with same leader arrives', async () => {
      const { peerId, leadership, localMembership } = await getStartedLeadershipWithSelfAsLeader()
      leadership.deliverGossipMessage(localMembership.state(), 'membership hash', { leader: peerId })
      expectKnownState(leadership, peerId)
    })

    it('From Known state should not change state if full gossip message with same leader arrives', async () => {
      const { peerId, leadership, localMembership } = await getStartedLeadershipWithSelfAsLeader()
      const remote = generateGossipMessage(peerId)
      leadership.deliverGossipMessage(localMembership.state(), remote.message, { leader: peerId })
      expectKnownState(leadership, peerId)
    })

    it('From Known state should not change state if gossip summary message arrives with different leader but no votes', async () => {
      const { peerId, leadership, localMembership, epochVoters } = await getStartedLeadershipWithSelfAsLeader()
      const remotePeerId = randomB58String()
      leadership.deliverGossipMessage(localMembership.state(), 'membership hash', { leader: remotePeerId })

      // Local peer should be in known state
      expect(leadership.getLeader()).to.equal(peerId)
      expect(leadership.getState()).to.equal(LeadershipState.Known)

      // But it should send a gossip message with voter state on next tick
      // anyway, because it knows there is a peer that does not have full state
      expect(leadership.needsUrgentBroadcast()).to.equal(true)
      expect(leadership.getGossipMessage(true)).to.deep.equal({
        leader: peerId,
        epochVoters: epochVoters.state()
      })
    })

    it('From Known state should not change state if full gossip message arrives with different leader but no votes', async () => {
      const { peerId, leadership, localMembership, epochVoters } = await getStartedLeadershipWithSelfAsLeader()
      const remote = generateGossipMessage()
      leadership.deliverGossipMessage(localMembership.state(), remote.message, { leader: remote.peerId })

      // Local peer should be in known state
      expect(leadership.getLeader()).to.equal(peerId)
      expect(leadership.getState()).to.equal(LeadershipState.Known)

      // But it should send a gossip message with voter state on next tick
      // anyway, because it knows there is a peer that does not have full state
      expect(leadership.needsUrgentBroadcast()).to.equal(true)
      expect(leadership.getGossipMessage(true)).to.deep.equal({
        leader: peerId,
        epochVoters: epochVoters.state()
      })
    })

    it('From Known state should not change to voting state if gossip message arrives with votes for self', async () => {
      const { peerId, leadership, localMembership } = await getStartedLeadershipWithSelfAsLeader()
      const remote = generateGossipMessage(peerId)
      leadership.deliverGossipMessage(localMembership.state(), remote.message, {
        leader: peerId,
        epochVoters: remote.epochVoters.state()
      })
      expectKnownState(leadership, peerId)
    })

    it('From Known state should move to Voting if full gossip message arrives with votes for different leader', async () => {
      const { leadership, localMembership, epochVoters } = await getStartedLeadershipWithSelfAsLeader()

      const remote = generateGossipMessage()
      leadership.deliverGossipMessage(localMembership.state(), remote.message, {
        leader: remote.peerId,
        epochVoters: remote.epochVoters.state()
      })

      const mergedEpochVoters = mergeEpochVoters(epochVoters, remote.epochVoters)
      expectVotingState(leadership, mergedEpochVoters.state())
    })

    it('From Known state should send voting information if gossip message arrives indicating someone doesnt have full voting information', async () => {
      const { peerId, leadership, localMembership, epochVoters } = await getStartedLeadershipWithSelfAsLeader()

      // Remote votes for local peer
      const remote = generateGossipMessage(peerId)
      leadership.deliverGossipMessage(localMembership.state(), remote.message, {
        leader: null, // Leader null means remote hasn't chosen leader yet
        epochVoters: remote.epochVoters.state()
      })

      // Local peer should be in known state
      expect(leadership.getLeader()).to.equal(peerId)
      expect(leadership.getState()).to.equal(LeadershipState.Known)

      // But it should send a gossip message with voter state on next tick
      // anyway, because it knows there is a peer that does not have full state
      const mergedEpochVoters = mergeEpochVoters(epochVoters, remote.epochVoters)
      expect(leadership.needsUrgentBroadcast()).to.equal(true)
      expect(leadership.getGossipMessage(true)).to.deep.equal({
        leader: peerId,
        epochVoters: mergedEpochVoters.state()
      })
    })
  })

  describe('Moving to new epoch', function () {
    // If there are remote membership changes the local node didn't know
    // about, and there's no remote vote CRDT, that means a membership change
    // happened while voting was in progress, so move to a new voting epoch
    it('Should move to new epoch on message having membership changes but no voting information', async () => {
      // Leadership is in Discovery state
      const leadership = createLeadership()
      const peerId = randomB58String()
      const localMembership = ORMap(peerId)
      localMembership.applySub(peerId, 'mvreg', 'write', [])
      leadership.start(peerId)
      expect(leadership._getEpochNumber()).to.equal(1)

      // Send new membership information (remote.message contains new remote peer),
      // don't send voting information
      const remote = generateGossipMessage()
      leadership.deliverGossipMessage(localMembership.state(), remote.message)

      // Expect to move to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, 2)

      // Leadership is now in Voting state - test same scenario from Voting state

      // Send new membership information (remote2.message contains new remote peer),
      // don't send voting information
      const remote2 = generateGossipMessage()
      leadership.deliverGossipMessage(localMembership.state(), remote2.message)

      // Expect to move to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, 3)
    })

    // If the local node has membership changes the remote doesn't have, but
    // the local node's epoch number is lower, something is out of sync so
    // move to a new voting epoch
    it('Should move to new epoch on message having remote with higher epoch but missing membership changes', async () => {
      // Leadership is in Known state
      const { peerId, leadership, localMembership } = await getStartedLeadershipWithSelfAsLeader()
      const lostLeadershipEvents = []
      leadership.on('lost leadership', id => lostLeadershipEvents.push(id))

      const remote = generateGossipMessage()

      // Move ahead of the local epoch
      remote.epochVoters.voteNewEpoch(remote.peerId)
      expect(leadership._getEpochNumber()).to.equal(1)
      expect((remote.epochVoters.value())[0]).to.equal(2)

      leadership.deliverGossipMessage(localMembership.state(), remote.message, {
        leader: remote.peerId,
        epochVoters: remote.epochVoters.state()
      })

      // Expect to move to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, 3)

      // Leadership is now in Voting state - test same scenario from Voting state

      // Move ahead of the local epoch
      const remote2 = generateGossipMessage()
      remote2.epochVoters.voteNewEpoch(remote2.peerId)
      remote2.epochVoters.voteNewEpoch(remote2.peerId)
      remote2.epochVoters.voteNewEpoch(remote2.peerId)
      expect(leadership._getEpochNumber()).to.equal(3)
      expect((remote2.epochVoters.value())[0]).to.equal(4)

      leadership.deliverGossipMessage(localMembership.state(), remote2.message, {
        leader: remote2.peerId,
        epochVoters: remote2.epochVoters.state()
      })

      // Expect to move to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, 5)
      expect(lostLeadershipEvents).to.deep.equal([peerId])
    })

    // If the remote peer knows about this peer, but didn't realize the local
    // peer was leader, move to a new voting epoch
    it('Should move to new epoch on message having remote with higher epoch that doesnt know local is leader', async () => {
      // Leadership is in Known state
      const { peerId, leadership, localMembership } = await getStartedLeadershipWithSelfAsLeader()
      expect(leadership._getEpochNumber()).to.equal(1)

      const remotePeerId = randomB58String()

      // Move ahead of the local epoch
      const remoteEpochVoters = EpochVoters(remotePeerId)
      remoteEpochVoters.vote(remotePeerId)
      remoteEpochVoters.voteNewEpoch(remotePeerId)
      expect((remoteEpochVoters.value())[0]).to.equal(2)

      // Remote knows about local peer (but not that self is leader)
      const remoteMembership = ORMap(remotePeerId)
      remoteMembership.applySub(remotePeerId, 'mvreg', 'write', [])
      remoteMembership.applySub(peerId, 'mvreg', 'write', [])

      const message = ['collab', remoteMembership.state(), 'rga']
      leadership.deliverGossipMessage(localMembership.state(), message, {
        leader: remotePeerId,
        epochVoters: remoteEpochVoters.state()
      })

      // Expect to move to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, 3)
    })
  })

  describe('Voting', function () {
    function peer () {
      const pid = randomB58String()
      const epochVoters = EpochVoters(pid)
      const membership = ORMap(pid)
      membership.applySub(pid, 'mvreg', 'write', [])
      return { peerId: pid, membership, epochVoters }
    }

    it('Should elect leader with majority of votes', async () => {
      const { peerId, leadership, localMembership, epochVoters } = await getStartedLeadershipWithSelfAsLeader()

      const leaderEvents = []
      leadership.on('leader', id => leaderEvents.push(id))
      const lostLeadershipEvents = []
      leadership.on('lost leadership', id => lostLeadershipEvents.push(id))

      // Two votes for peer 1, one for local peer
      const p1 = peer()
      p1.epochVoters.vote(p1.peerId)
      const p2 = peer()
      p2.epochVoters.vote(p1.peerId)
      p2.membership.apply(p1.membership.state())
      p2.epochVoters.apply(p1.epochVoters.state())

      const message = ['collab', p2.membership.state(), 'rga']
      leadership.deliverGossipMessage(localMembership.state(), message, {
        leader: null, // Leader null means remote hasn't chosen leader yet
        epochVoters: p2.epochVoters.state()
      })

      // Expect peer 1 to be voted leader
      expect(leadership.getLeader()).to.equal(p1.peerId)
      expect(leadership.getState()).to.equal(LeadershipState.Known)
      expect(lostLeadershipEvents).to.deep.equal([peerId])
      expect(leaderEvents).to.deep.equal([p1.peerId])

      // But local peer should emit a gossip message with voter state on next
      // tick, because it knows there is a peer that does not have full state
      let mergedEpochVoters = mergeEpochVoters(epochVoters, p2.epochVoters)
      expect(leadership.needsUrgentBroadcast()).to.equal(true)
      expect(leadership.getGossipMessage(false)).to.deep.equal({ leader: p1.peerId })
      expect(leadership.getGossipMessage(true)).to.deep.equal({
        leader: p1.peerId,
        epochVoters: mergedEpochVoters.state()
      })

      // When this peer detects the leader has been evicted, it should move to
      // a new epoch
      expect(leadership._getEpochNumber()).to.equal(1)
      leadership._membership.emit('peer left', p1.peerId)
      await new Promise(resolve => setTimeout(resolve))
      expectVotingStateNewEpochNumber(leadership, 2)
    })

    it('If there is a tie, should wait for votes then elect self repeating with exponential back off', async () => {
      const leadershipElectionGossipNowMaxCount = 10
      const gfh = mock.gossipFrequencyHeuristic()
      const leadership = createLeadership({
        leadershipElectionGossipNowMaxCount
      }, gfh)
      const peerId = randomB58String()
      const localMembership = ORMap(peerId)
      localMembership.applySub(peerId, 'mvreg', 'write', [])

      // Wait for start up
      leadership.start(peerId)
      for (let i = 0; i < leadershipElectionGossipNowMaxCount; i++) {
        gfh.emit('gossip now')
      }
      await new Promise(resolve => setTimeout(resolve))

      // Cast vote for local peer
      const epochVoters = EpochVoters(peerId)
      epochVoters.vote(peerId)

      // One vote for remote peer, one for local peer
      const remotePeer = peer()
      remotePeer.epochVoters.vote(remotePeer.peerId)

      let message = ['collab', remotePeer.membership.state(), 'rga']
      leadership.deliverGossipMessage(localMembership.state(), message, {
        leader: null,
        epochVoters: remotePeer.epochVoters.state()
      })

      // Tie - expect to still be in the voting state
      let mergedEpochVoters = mergeEpochVoters(epochVoters, remotePeer.epochVoters)
      expectVotingState(leadership, mergedEpochVoters.state())

      // Expect to wait 0 ticks the first time
      await new Promise(resolve => setTimeout(resolve))

      // Expect to have moved to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, mergedEpochVoters.value()[0] + 1)

      async function voteInNewEpoch (maxTickCount) {
        // Remote peer moves to new epoch and votes for itself again
        // In the new epoch: one vote for remote peer, one for local peer
        remotePeer.epochVoters.voteNewEpoch(remotePeer.peerId)
        message = ['collab', remotePeer.membership.state(), 'rga']
        leadership.deliverGossipMessage(localMembership.state(), message, {
          leader: null,
          epochVoters: remotePeer.epochVoters.state()
        })

        // Tie - expect local peer to still be in the voting state
        epochVoters.voteNewEpoch(peerId)
        mergedEpochVoters = mergeEpochVoters(epochVoters, remotePeer.epochVoters)
        expectVotingState(leadership, mergedEpochVoters.state())

        // Emit enough gossip now events such that local peer stops waiting
        // and votes for itself in a new epoch
        for (let i = 0; i < maxTickCount; i++) {
          gfh.emit('gossip now')
        }
        await new Promise(resolve => setTimeout(resolve))

        // Expect to have moved to a new epoch with a vote for self
        expectVotingStateNewEpochNumber(leadership, mergedEpochVoters.value()[0] + 1)
      }

      // Exponential backoff

      // Expect to wait 0 or 1 ticks the second time: 2^1 - 1
      await voteInNewEpoch(1)
      // Expect to wait 0 to 3 ticks the third time: 2^2 - 1
      await voteInNewEpoch(3)
      // Expect to wait 0 to 7 ticks the third time: 2^3 - 1
      await voteInNewEpoch(7)
      // Expect to wait 0 to 15 ticks the third time: 2^4 - 1
      await voteInNewEpoch(15)

      // Eventually with the two peers backing off, one of the peers will
      // receive the other's vote before voting for itself.
      // In this case let's simulate the scenario where the remote peer
      // receives the local peer's state, then casts its vote in the
      // same epoch
      const localGossip = leadership.getGossipMessage(true)
      remotePeer.epochVoters.apply(localGossip.epochVoters)
      remotePeer.epochVoters.vote(peerId)

      const leaderEvents = []
      leadership.on('leader', id => leaderEvents.push(id))
      const wonLeadershipEvents = []
      leadership.on('won leadership', id => wonLeadershipEvents.push(id))

      // Now the local peer gets a message from the remote peer
      message = ['collab', remotePeer.membership.state(), 'rga']
      leadership.deliverGossipMessage(localMembership.state(), message, {
        leader: null,
        epochVoters: remotePeer.epochVoters.state()
      })

      // Expect that the local peer is now leader
      expectKnownState(leadership, peerId)
      expect(leaderEvents).to.deep.equal([peerId])
      expect(wonLeadershipEvents).to.deep.equal([peerId])
    })

    it('If there are not enough votes, should wait for votes then elect self', async () => {
      const leadershipElectionGossipNowMaxCount = 3
      const gfh = mock.gossipFrequencyHeuristic()
      const leadership = createLeadership({
        leadershipElectionGossipNowMaxCount
      }, gfh)

      const peerId = randomB58String()
      const localMembership = ORMap(peerId)
      localMembership.applySub(peerId, 'mvreg', 'write', [])

      // Wait for start up
      leadership.start(peerId)
      for (let i = 0; i < leadershipElectionGossipNowMaxCount; i++) {
        gfh.emit('gossip now')
      }
      await new Promise(resolve => setTimeout(resolve))

      // Membership: local peer + 3 others = 4 peers
      // 1 vote for peer 1, one for local peer
      const p1 = peer()
      p1.epochVoters.vote(p1.peerId)
      const p2 = peer()
      const p3 = peer()
      p1.membership.apply(p2.membership.state())
      p1.membership.apply(p3.membership.state())

      const message = ['collab', p1.membership.state(), 'rga']
      leadership.deliverGossipMessage(localMembership.state(), message, {
        leader: null,
        epochVoters: p1.epochVoters.state()
      })

      // Simulate updated peer count
      leadership._membership.peerCount = () => 4

      // Not enough votes for majority (2 / 4) - expect to still be in the
      // voting state
      const epochVoters = EpochVoters(peerId)
      epochVoters.vote(peerId)
      let mergedEpochVoters = mergeEpochVoters(epochVoters, p1.epochVoters)

      // Expect to wait enough ticks until the tick timer triggers and the local
      // peer votes in a new epoch
      for (let i = 0; i < leadershipElectionGossipNowMaxCount; i++) {
        expectVotingState(leadership, mergedEpochVoters.state())
        gfh.emit('gossip now')
        await new Promise(resolve => setTimeout(resolve))
      }

      // Expect to have moved to a new epoch with a vote for self
      expectVotingStateNewEpochNumber(leadership, mergedEpochVoters.value()[0] + 1)
    })
  })
})
