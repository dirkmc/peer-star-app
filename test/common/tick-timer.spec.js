/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const TickTimer = require('../../src/common/tick-timer')

describe('tick timer', () => {
  it('waits correct number of ticks', (done) => {
    const tt = new TickTimer()
    let resolved = 0
    let ticks = 0

    // Should resolve immediately
    tt.waitForTicks('negative', -1).then(result => {
      expect(ticks).to.equal(0)
      expect(result).to.equal(true)
      resolved++
    })
    tt.waitForTicks('zero', 0).then(result => {
      expect(ticks).to.equal(0)
      expect(result).to.equal(true)
      resolved++
    })

    // Should resolve after first tick
    tt.waitForTicks('one', 1).then(result => {
      expect(ticks).to.equal(1)
      expect(result).to.equal(true)
      resolved++
    })

    // Should resolve after second tick
    tt.waitForTicks('two', 2).then(result => {
      expect(ticks).to.equal(2)
      expect(result).to.equal(true)
      resolved++
    })

    // Should resolve after third tick
    tt.waitForTicks('three', 3).then(result => {
      expect(ticks).to.equal(3)
      expect(result).to.equal(true)
      resolved++
    })

    function next() {
      ticks++
      tt.tick()
      if (ticks < 5) {
        return setTimeout(next)
      }
      expect(resolved).to.equal(5)
      done()
    }
    setTimeout(next)
  })

  it('resolves to false if there is already a timer with that id', (done) => {
    const tt = new TickTimer()
    let ticks = 0

    let resolved = 0
    tt.waitForTicks('unique id', 1).then(result => {
      expect(ticks).to.equal(1)
      expect(result).to.equal(true)
      resolved++
    })
    expect(tt.hasTimer('unique id')).to.equal(true)

    setTimeout(() => {
      expect(tt.hasTimer('unique id')).to.equal(true)
      tt.waitForTicks('unique id', 1).then(result => {
        expect(result).to.equal(false)
        resolved++
      })
      ticks++
      tt.tick()

      setTimeout(() => {
        expect(tt.hasTimer('unique id')).to.equal(false)
        let newTicks = 0
        // Now that timer has resolved, a new one can be
        // created with the same id
        tt.waitForTicks('unique id', 1).then(result => {
          expect(newTicks).to.equal(1)
          expect(result).to.equal(true)
          resolved++
        })
        setTimeout(() => {
          expect(tt.hasTimer('unique id')).to.equal(true)
          newTicks++
          tt.tick()
          setTimeout(() => {
            expect(tt.hasTimer('unique id')).to.equal(false)
            expect(resolved).to.equal(3)
            done()
          })
        })
      })
    })
  })

  it('immediately resolves to false if tick is cleared', (done) => {
    const tt = new TickTimer()
    let ticks = 0

    let resolved = 0
    tt.waitForTicks('id1', 2).then(result => {
      expect(result).to.equal(false)
      resolved++
    })
    tt.waitForTicks('id2', 3).then(result => {
      expect(result).to.equal(false)
      resolved++
    })

    tt.clearTimers()
    setTimeout(() => {
      expect(resolved).to.equal(2)
      done()
    })
  })
})
