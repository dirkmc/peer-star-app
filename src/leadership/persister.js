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
    this._persisterEras = []
    this._running = false

    this._membership.leadership.on('won leadership', () => this.onElected())
    this._membership.leadership.on('lost leadership', () => this.onDeposed())
  }

  start () {
    this._running = true
    this.emit('started')
  }

  _awaitStart() {
    if (this._running) {
      return
    }
    if (this._starting) {
      return this._starting
    }
    this._starting = new Promise(resolve => {
      this.once('started', resolve)
    })
    return this._starting
  }

  _getEra() {
    return this._persisterEras.length
  }

  async onElected () {
    await this._awaitStart()
    let era = this._getEra()
    debug('Elected leader, creating persister for era %d', era)

    // In theory it's possible that if there are a series of
    // won leadership / lost leadership events in quick succession, multiple
    // persisters could be started so make sure they're all shut down before
    // creating a new one
    await this._stopAllPersisters()
    if (this._getEra() !== era) {
      return
    }

    const persister = Persister(this._ipfs, this._name, this._type, this._store, this._options)
    this._persisterEras.push(persister)
    era = this._getEra()

    debug('Fetching state')
    const state = await persister.fetchLatestState()
    debug('Got state', state)
    if (this._getEra() !== era) {
      return
    }
    if (state) {
      debug('Saving state to local store')
      await this._store.saveStates([state.clock, new Map([[null, state.state]])])
      if (this._getEra() !== era) {
        return
      }
    }
    debug('Starting persister')
    await persister.start()
    if (this._getEra() !== era) {
      return
    }
    debug('Persister started')
    this.emit('persistence started')
  }

  _stopAllPersisters () {
    return Promise.all(this._persisterEras.map(async (p, i) => {
      // Note: calling stop() multiple times is ok
      await p && p.stop()
      // Clean up memory (note: doesn't change array length)
      delete this._persisterEras[i]
    }))
  }

  async onDeposed () {
    debug('Deposed from leadership, stopping persister')
    await this._stopAllPersisters()
    this.emit('persistence stopped')
  }

  async stop() {
    await this._stopAllPersisters()
    this._running = false
    this.emit('stopped')
  }
}
