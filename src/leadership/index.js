'use strict'

const debug = require('debug')('peer-star:leadership')
const EventEmitter = require('events')
const CRDT = require('delta-crdts')
require('./epoch-voters-crdt')
const EpochVoters = CRDT('epochvoters')
const Voting = require('./voting')
const membershipUtil = require('../common/membership-util')
const TickTimer = require('../common/tick-timer')
const Options = require('../common/options')

const LeadershipState = {
  // We just started up and don't yet know who the leader is
  Discovery: 'Discovery',
  // We are currently voting on who the leader should be
  Voting: 'Voting',
  // We know who the leader is
  Known: 'Known'
}

const defaultOptions = {
  // After this many 'gossip now' events without a response, this peer will
  // assume it is the leader
  leadershipElectionGossipNowMaxCount: 20
}

class Leadership extends EventEmitter {
  constructor (membership, gossipFrequencyHeuristic, options) {
    super()

    this._membership = membership
    this._gossipFrequencyHeuristic = gossipFrequencyHeuristic
    this._options = Options.merge(defaultOptions, options)

    this._leader = undefined
    this._someoneNeedsToKnowVotes = false
    this._leadershipState = LeadershipState.Discovery

    this._backoffMaxTicks = 1
    this._tickTimer = new TickTimer()

    this._gossipNow = this._gossipNow.bind(this)
    this._onPeerLeft = this._onPeerLeft.bind(this)

    this._running = false
    this.dbg = (...args) => debug('%s:', this._peerId, ...args)
  }

  getLeader () {
    return this._leader
  }

  getState () {
    return this._leadershipState
  }

  start (peerId) {
    this._peerId = peerId    
    this._gossipFrequencyHeuristic.on('gossip now', this._gossipNow)
    this._membership.on('peer left', this._onPeerLeft)
    this._epochVoters = EpochVoters(this._peerId)
    this._waitForVotesThenVoteForSelf()
    this._running = true
    this.dbg('started')
  }

  stop () {
    if (!this._running) {
      return
    }
    this._gossipFrequencyHeuristic.removeListener('gossip now', this._gossipNow)
    this._membership.removeListener('peer left', this._onPeerLeft)
    this._tickTimer.clearTimers()
    this._running = false
    this.dbg('stopped')
  }

  // Indicates whether this peer wants to gossip out information to other peers
  needsUrgentBroadcast () {
    return this._leadershipState === LeadershipState.Voting || this._someoneNeedsToKnowVotes
  }

  // Get the gossip message for leadership
  // Can be a full message or just a summary
  getGossipMessage (fullMessage = false) {
    const haveLeader = this._leadershipState === LeadershipState.Known
    const message = {
      leader: haveLeader ? this._leader : null,
    }

    if (fullMessage && this.needsUrgentBroadcast()) {
      message.epochVoters = this._epochVoters.state()
      this._someoneNeedsToKnowVotes = false
    }
    return message
  }

  _onPeerLeft (peerId) {
    // If the evicted peer was the leader, vote for a new leader
    if (this._leader === peerId) {
      this.dbg('leader %s was evicted, voting in new epoch', this._leader)
      this._voteNewEpoch()
    }
  }

  deliverGossipMessage (localMembership, message, leadershipMsg = {}) {
    const remoteMembership = (message || [])[1]
    const remoteLeader = leadershipMsg.leader
    if (!localMembership || !message || (remoteLeader && !remoteMembership)) {
      this.dbg('ignoring badly formatted message')
      return
    }

    const remoteEpochVotersState = leadershipMsg.epochVoters
    this.dbg('got gossip message', message, leadershipMsg)

    // Check if we were waiting to hear from another peer to find out who the
    // leader is and we've now discovered
    if (this._leadershipState === LeadershipState.Discovery && remoteLeader) {
      this.dbg(`discovered leader is ${remoteLeader}`)
      this._setLeader(remoteLeader)
    }

    if (this._leadershipState === LeadershipState.Known && !remoteLeader && remoteEpochVotersState) {
      // If we have finished voting but someone else hasn't, make sure we
      // broadcast the voting information in our next gossip message
      this.dbg(`local knows leader but remote does not, make sure we send voting state on next tick`)
      this._someoneNeedsToKnowVotes = true
    }

    // If this is a summary message, it doesn't include membership or voting
    // information so there's nothing more to do
    if (!remoteMembership || typeof remoteMembership === 'string') {
      this.dbg(`no remote membership state or votes received, taking no further action`)
      return
    }

    // If the remote has not sent us voting information
    if (!remoteEpochVotersState) {
      // If we have a leader and there are no new votes, nothing more to do
      if (this._leadershipState === LeadershipState.Known) {
        this.dbg(`leader known and no votes received`)
        return
      }

      // If there are remote membership changes the local node didn't know
      // about, and there's no remote vote CRDT, that means a membership change
      // happened while voting was in progress, so move to a new voting epoch
      const localNeedsUpdate = !membershipUtil.firstSubsumesSecond(localMembership, remoteMembership)
      if (localNeedsUpdate) {
        this.dbg(`membership changes occurred while voting was in progress`)
        return this._voteNewEpoch()
      }

      this.dbg(`no votes received and no membership changes, no further action taken`)
      return
    }

    // Save the local and remote epoch voters value before merging
    const localValueBefore = this._epochVoters.value()
    const remoteEpochVoters = EpochVoters('tmp')
    remoteEpochVoters.apply(remoteEpochVotersState)
    const remoteValueBefore = remoteEpochVoters.value()

    // Merge in the remote votes with local votes
    this._epochVoters.apply(remoteEpochVotersState)

    // Check if any votes have changed
    if (Voting.epochVotersIdentical(localValueBefore, this._epochVoters.value())) {
      this.dbg(`no change in votes - ignoring`)
      return
    }

    // Something has changed, so clear the tick timer
    this._tickTimer.clearTimers()

    // If the local node has membership changes the remote doesn't have, but the
    // local node's epoch number is lower, something is out of sync so move to
    // a new voting epoch
    const localEpoch = localValueBefore[0]
    const remoteEpoch = remoteValueBefore[0]
    const remoteNeedsUpdate = !membershipUtil.firstSubsumesSecond(remoteMembership, localMembership)
    if (remoteNeedsUpdate && localEpoch < remoteEpoch) {
      this.dbg(`remote epoch is higher but remote does not have local changes`)
      return this._voteNewEpoch()
    }

    // If the remote epoch is greater than the one we were part of, the current
    // leader must be replaced
    if (remoteEpoch > localEpoch) {
      // If the current leader is self, then someone thought this node
      // was offline, but it is online, so increment the epoch and vote
      // for self
      if (this._leader === this._peerId) {
        this.dbg(`remote epoch is higher but remote didnt know self was leader`)
        return this._voteNewEpoch()
      }

      this._setStateVoting()
    }

    // Vote for a candidate
    Voting.vote(this._epochVoters, this._peerId)

    // Check if a quorum has now been reached
    const leaderSoFar = Voting.getLeader(this._epochVoters)

    // Merge membership
    const members = membershipUtil.crdtWithState(localMembership)
    members.apply(remoteMembership)
    const memberCount = Object.keys(members.value()).length

    // If a majority of peers have voted for the leader, the vote is complete
    if (leaderSoFar && leaderSoFar.votes > memberCount / 2) {
      this.dbg(`leader elected with ${leaderSoFar.votes} votes - ${leaderSoFar.leader}`)
      this._setLeader(leaderSoFar.leader)
      return
    }

    // Make sure we're in the voting state
    this._setStateVoting()

    // If there is a tie so far
    if (leaderSoFar === null) {
      // If all members have already voted, back off for a random time period,
      // then start a new epoch and vote for self
      const votesSoFar = this._epochVoters.value()[1].size
      if (votesSoFar >= memberCount) {
        this.dbg(`all members have voted and there is a tie`)
        return this._backOffThenVoteNewEpoch()
      }

      // Otherwise wait for new votes to come in, if none arrive then vote for self
      this.dbg(`tied so far, waiting for more votes`)
      return this._waitForVotesThenVoteForSelf()
    }

    // Otherwise wait for new votes to come in, if none arrive then vote for self
    this.dbg(`not enough votes for leader yet (${leaderSoFar.votes} / ${memberCount}) - waiting for more votes`)
    this._waitForVotesThenVoteForSelf()
  }

  _voteNewEpoch () {
    this._tickTimer.clearTimers()
    this._setStateVoting()
    this._epochVoters.voteNewEpoch(this._peerId)
    const epoch = this._epochVoters.value()[0]
    this.dbg(`voted for self in new epoch ${epoch}`)
    this._waitForVotesThenVoteForSelf()
  }

  _setStateVoting() {
    const selfWasLeader = this._leader === this._peerId

    this._leader = null
    this._leadershipState = LeadershipState.Voting

    if (selfWasLeader) {
      this.emit('lost leadership', this._peerId)
    }
  }

  _setLeader (peerId) {
    this._backoffMaxTicks = 1
    this._tickTimer.clearTimers()
    if (this._leader !== peerId || this._leadershipState !== LeadershipState.Known) {
      const selfWasLeader = this._leader === this._peerId

      this._leadershipState = LeadershipState.Known
      this._leader = peerId

      if (selfWasLeader) {
        this.emit('lost leadership', this._peerId)
      }
      this.emit('leader', this._leader)
      if (this._leader === this._peerId) {
        this.dbg('self elected leader:', this._peerId)
        this.emit('won leadership', this._peerId)
      } else {
        this.dbg('new leader elected:', this._leader)
      }
    }
  }

  _gossipNow () {
    this._tickTimer.tick()
  }

  // Wait a few ticks to see if we hear from another peer
  // If not then vote in a new epoch
  async _backOffThenVoteNewEpoch () {
    // The number of ticks will be between 0 and this._backoffMaxTicks
    // The first time this._backoffMaxTicks is 1, so the timer
    // should resolve immediately (without waiting for a tick)
    const ticks = Math.floor(Math.random() * this._backoffMaxTicks)
    const realMax = this._backoffMaxTicks - 1
    this.dbg(`backing off ${ticks} (max ${realMax}) ticks before voting for self in new epoch`)

    // Exponentially increase the back off time period
    // (this gets reset to 1 when a leader is elected)
    this._backoffMaxTicks *= 2
    const timerCompleted = await this._tickTimer.waitForTicks('backoff', ticks)
    if (timerCompleted) {
      // Timer completed without being cleared so vote in new epoch
      this.dbg(`didnt hear from another peer after ${ticks} ticks`)
      this._voteNewEpoch()
    }
  }

  async _waitForVotesThenVoteForSelf () {
    // Wait for a while to see if we hear from another peer
    const ticks = this._options.leadershipElectionGossipNowMaxCount
    const timerCompleted = await this._tickTimer.waitForTicks('vote-self', ticks)
    if (!timerCompleted) {
      // Timer was cleared
      return
    }

    // If we just started up and never heard from anyone, or if we're the
    // only peer in the collaboration, then assume leadership
    const peerCount = this._membership.peerCount()
    if (this._leadershipState === LeadershipState.Discovery || peerCount === 1) {
      this.dbg(`self is only peer, assuming leadership`)
      Voting.vote(this._epochVoters, this._peerId)
      this._setLeader(this._peerId)
    } else if (this._leadershipState === LeadershipState.Voting) {
      // There was a leader at some stage, but voting got stuck,
      // so move on to a new epoch and vote for self
      this.dbg(`voting got stuck, voting in new epoch`)
      this._voteNewEpoch()
    }
  }

  // Only used by tests
  _getEpochNumber() {
    return this._epochVoters.value()[0]
  }
}

Leadership.LeadershipState = LeadershipState

module.exports = Leadership
