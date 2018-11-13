'use strict'

class TickTimer {
  constructor () {
    this._tickTimers = new Map()
  }

  waitForTicks (id, count) {
    return new Promise(resolve => {
      if (this._tickTimers.has(id)) {
        return resolve(false)
      }
      if (count <= 0) {
        return resolve(true)
      }
      this._tickTimers.set(id, { id, resolve, count })
    })
  }

  hasTimer (id) {
    return this._tickTimers.has(id)
  }

  clearTimers () {
    for (const timer of this._tickTimers.values()) {
      timer.resolve(false)
    }
    this._tickTimers.clear()
  }

  tick () {
    for (const timer of this._tickTimers.values()) {
      timer.count--
      if (timer.count <= 0) {
        timer.resolve(true)
        this._tickTimers.delete(timer.id)
      }
    }
  }
}

module.exports = TickTimer
