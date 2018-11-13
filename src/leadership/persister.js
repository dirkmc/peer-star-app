'use strict'

const debug = require('debug')('peer-star:leadership:persistence')
const EventEmitter = require('events')
const Persister = require('../persister')

module.exports = class LeadershipPersister extends EventEmitter {
  constructor (collaboration, membership, ipfs, name, type, store, options) {
    super()

    this._collaboration = collaboration
    this._membership = membership
    this._ipfs = ipfs
    this._name = name
    this._type = type
    this._store = store
    this._options = options

    this._epoch = 0
    this._membership.leadership.on('won leadership', () => this.onElected())
    this._membership.leadership.on('lost leadership', () => this.onDeposed())
  }

  async onElected () {
    this._epoch++
    const epoch = this._epoch
    debug('elected leader, creating persister')
    this._persister = Persister(this._ipfs, this._name, this._type, this._store, this._options)
    debug('fetching state')
    const state = await this._persister.fetchLatestState()
    debug('got state', state)
    if (this._epoch !== epoch) {
      return
    }
    if (state) {
      debug('saving state to local store')
      await this._store.saveDelta([null, state.clock, state.state])
      if (this._epoch !== epoch) {
        return
      }
    }
    debug('starting persister')
    await this._persister.start()
    debug('persister started')
    this.emit('started')
  }

  async onDeposed () {
    debug('deposed from leadership, stopping persister')
    this._epoch++
    await (this._persister && this._persister.stop())
    this.emit('stopped')
  }

  async stop() {
    await this._persister.stop()
    this.emit('stopped')
  }
}
