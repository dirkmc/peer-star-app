'use strict'

const EventEmitter = require('events')
const Options = require('./options')

const defaultOptions = {
  interval: 1000
}

class Ticker extends EventEmitter {
  constructor (options) {
    super()
    this._options = Options.merge(defaultOptions, options)
  }

  start () {
    if (this._interval) {
      return
    }
    this._interval = setInterval(() => {
      this.emit('tick')
    }, this._options.interval)
  }

  stop () {
    clearInterval(this._interval)
    this._interval = null
  }
}

module.exports = Ticker
