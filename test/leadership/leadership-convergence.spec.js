/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const { decode } = require('delta-crdts-msgpack-codec')
const FakePeerInfo = require('../utils/fake-peer-info')
const randomPeerId = require('../utils/random-peer-id').buffer
const EventEmitter = require('events')
const Leadership = require('../../src/leadership')
const LeadershipState = Leadership.LeadershipState
const Membership = require('../../src/collaboration/membership')
const Multiaddr = require('multiaddr')

const mock = {
  ipfs () {
    return {
      _peerInfo: new FakePeerInfo(randomPeerId()),
      id () {
        return Promise.resolve({ id: this._peerInfo.id.toB58String() })
      }
    }
  },
  connectionManager () {
    const cm = new EventEmitter()
    cm.start = () => {}
    cm.stop = () => {}
    return cm
  }
}

class MockNetwork {
  constructor () {
    this.memberships = []
    this._running = true
  }

  stop () {
    this._running = false
  }

  async evictMember (id) {
    const index = this.memberships.findIndex(m => m._peerId === id)
    const [evicted] = this.memberships.splice(index, 1)
    await evicted.stop()

    // Resolve after the first other member has detected the eviction
    return new Promise(resolve => {
      for (const m of this.memberships) {
        setTimeout(() => {
          m.connectionManager.emit('should evict', evicted._ipfs._peerInfo)
          resolve()
        }, 20 + Math.random() * 30)
      }
    })
  }

  awaitConvergence () {
    return new Promise(async resolve => {
      let handled = false
      const waitForConvergence = () => {
        if (handled) {
          return
        }
        handled = true
        if (this.hasConverged()) {
          return resolve()
        }
        this._pollForConvergence(resolve)
      }

      // Check after the first 'leader' event
      for (const m of this.memberships) {
        m.leadership.once('leader', waitForConvergence)
      }
      // If the 'leader' event happened too fast, we might have missed it, so
      // check after a delay as well
      setTimeout(waitForConvergence, 100)
    })
  }

  async _pollForConvergence (resolve) {
    while (this._running) {
      if (this.hasConverged()) {
        return resolve()
      }
      await new Promise(r => setTimeout(r, 10))
    }
  }

  hasConverged () {
    const counts = {}
    for (const mi of this.memberships) {
      const leaderId = mi.leadership.getLeader()
      counts[leaderId] = (counts[leaderId] || 0) + 1
    }
    const keys = Object.keys(counts)
    return keys.length === 1 && !!counts[keys[0]]
  }

  async createMembership (opts) {
    const network = this
    const ipfs = mock.ipfs()
    const app = {
      peerCountGuess () {
        return network.memberships.length
      },
      gossip (message) {
        message = decode(message)
        const [collabName, membershipMessage] = message
        for (let otherMembership of network.memberships) {
          if (otherMembership !== membership) {
            setTimeout(() => {
              otherMembership.deliverGossipMessage(message)
            }, opts.AvgNetworkDelay * 0.25 + opts.AvgNetworkDelay * 1.5 * Math.random())
          }
        }
      }
    }
    const collaboration = {
      name: 'collab name',
      typeName: 'gset'
    }
    const store = {}
    const clocks = {}
    const options = Object.assign({
      peerIdByteCount: 32,
      preambleByteCount: 2,
      keys: {},
      leadershipEnabled: true
    }, opts)
    const replication = {}

    const memberIndex = this.memberships.length
    ipfs._peerInfo.multiaddrs.add(Multiaddr(`/ip4/127.0.0.1/tcp/${memberIndex}`))

    const mOpts = Object.assign(options, {
      connectionManager: mock.connectionManager()
    })
    const membership = new Membership(ipfs, null, app, collaboration, store, clocks, replication, mOpts)
    this.memberships.push(membership)

    await membership.start()
    return membership
  }
}

describe('leadership convergence', function () {
  this.timeout(10000)

  let currentNetwork
  const opts = {
    leadershipElectionGossipNowMaxCount: 5,
    samplingIntervalMS: 10,
    targetGlobalMembershipGossipFrequencyMS: 10,
    AvgNetworkDelay: 1
  }

  async function createConnectedPeers(n, options) {
    currentNetwork = new MockNetwork()
    const o = Object.assign({}, opts, options)
    await Promise.all([...Array(n)].map(() => currentNetwork.createMembership(o)))
    return currentNetwork
  }

  afterEach(() => {
    currentNetwork.stop()
    return Promise.all(currentNetwork.memberships.map(m => m.stop()))
  })

  describe('start with n peers convergence', () => {
    it('elects only peer as leader when no other peer detected', async () => {
      const network = await createConnectedPeers(1)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
    })

    it('elects a peer as leader when starting with two peers', async () => {
      const network = await createConnectedPeers(2)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
    })

    it('elects a peer as leader when starting with ten peers', async () => {
      const network = await createConnectedPeers(10)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
    })
  })

  describe('convergence after membership change', () => {  
    it('elects new peer when one peer removed', async () => {
      const network = await createConnectedPeers(5)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
      const leader = network.memberships[0].leadership.getLeader()
      await network.evictMember(leader)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
    })

    it('elects new peer when membership changes occur while voting is in progress', async () => {
      const network = await createConnectedPeers(5)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
      const leader = network.memberships[0].leadership.getLeader()
      await network.evictMember(leader)
      await network.createMembership(opts)
      const leader2 = network.memberships[3].leadership.getLeader()
      await network.evictMember(leader2)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
    })

    it('elects self when all other peers are removed', async () => {
      const network = await createConnectedPeers(5)
      await network.awaitConvergence()
      expect(network.hasConverged()).to.equal(true)
      for (let i = 0; i < 4; i++) {
        const leader = network.memberships[0].leadership.getLeader()
        await network.evictMember(leader)
        await network.awaitConvergence()
        expect(network.hasConverged()).to.equal(true)
      }
    })
  })
})
