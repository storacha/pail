import { describe, it } from 'mocha'
import assert from 'node:assert'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/crdt/api.js'
import { advance, vis } from '../src/clock/index.js'
import { put, get, root, entries } from '../src/crdt/index.js'
import { Blockstore, randomCID } from './helpers.js'

describe('CRDT', () => {
  it('put a value to a new clock', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])
    const key = 'key'
    const value = await randomCID(32)
    const { event, head } = await alice.putAndVis(key, value)

    assert(event)
    assert(event.value.data.type === 'put')
    assert.equal(event.value.data.key, key)
    assert.equal(event.value.data.value.toString(), value.toString())
    assert.equal(head.length, 1)
    assert.equal(head[0].toString(), event.cid.toString())
  })

  it('linear put multiple values', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])

    const key0 = 'key0'
    const value0 = await randomCID(32)
    await alice.put(key0, value0)

    const key1 = 'key1'
    const value1 = await randomCID(32)
    const result = await alice.putAndVis(key1, value1)

    assert(result.event)
    assert(result.event.value.data.type === 'put')
    assert.equal(result.event.value.data.key, key1)
    assert.equal(result.event.value.data.value.toString(), value1.toString())
    assert.equal(result.head.length, 1)
    assert.equal(result.head[0].toString(), result.event.cid.toString())
  })

  it('simple parallel put multiple values', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])
    await alice.put('apple', await randomCID(32))
    const bob = new TestPail(blocks, alice.head)

    /** @type {Array<[string, API.UnknownLink]>} */
    const data = [
      ['banana', await randomCID(32)],
      ['kiwi', await randomCID(32)],
      ['mango', await randomCID(32)],
      ['orange', await randomCID(32)],
      ['pear', await randomCID(32)]
    ]

    const { event: aevent0 } = await alice.put(data[0][0], data[0][1])
    const { event: bevent0 } = await bob.put(data[1][0], data[1][1])

    assert(aevent0)
    assert(bevent0)

    const carol = new TestPail(blocks, bob.head)

    const { event: bevent1 } = await bob.put(data[2][0], data[2][1])
    const { event: cevent1 } = await carol.put(data[3][0], data[3][1])

    assert(bevent1)
    assert(cevent1)

    await alice.advance(cevent1.cid)
    await alice.advance(bevent0.cid)
    await alice.advance(bevent1.cid)
    await bob.advance(aevent0.cid)

    const { event: aevent1 } = await alice.putAndVis(data[4][0], data[4][1])

    assert(aevent1)

    await bob.advance(aevent1.cid)
    await carol.advance(aevent1.cid)

    assert(alice.root)
    assert(bob.root)
    assert(carol.root)
    assert.equal(bob.root.toString(), alice.root.toString())
    assert.equal(carol.root.toString(), alice.root.toString())

    // get item put to bob from alice
    const avalue = await alice.get(data[1][0])
    assert(avalue)
    assert.equal(avalue.toString(), data[1][1].toString())

    // get item put to alice from bob
    const bvalue = await bob.get(data[0][0])
    assert(bvalue)
    assert.equal(bvalue.toString(), data[0][1].toString())

    // get item put to alice from carol
    const cvalue = await bob.get(data[4][0])
    assert(cvalue)
    assert.equal(cvalue.toString(), data[4][1].toString())
  })

  it('get from multi event head', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])
    await alice.put('apple', await randomCID(32))
    const bob = new TestPail(blocks, alice.head)

    /** @type {Array<[string, API.UnknownLink]>} */
    const data = [
      ['banana', await randomCID(32)],
      ['kiwi', await randomCID(32)]
    ]

    await alice.put(data[0][0], data[0][1])
    const { event } = await bob.put(data[1][0], data[1][1])

    assert(event)

    await alice.advance(event.cid)
    const value = await alice.get(data[1][0])

    assert(value)
    assert.equal(value.toString(), data[1][1].toString())
  })

  it('entries from multi event head', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])
    await alice.put('apple', await randomCID(32))
    const bob = new TestPail(blocks, alice.head)

    /** @type {Array<[string, API.UnknownLink]>} */
    const data = [
      ['banana', await randomCID(32)],
      ['kiwi', await randomCID(32)]
    ]

    await alice.put(data[0][0], data[0][1])
    const { event } = await bob.put(data[1][0], data[1][1])

    assert(event)

    await alice.advance(event.cid)

    for await (const [k, v] of alice.entries()) {
      assert(v.toString(), new Map(data).get(k)?.toString())
    }
  })

  it('put same value to existing key', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])

    const key0 = 'key0'
    const value0 = await randomCID(32)

    const r0 = await alice.put(key0, value0)
    assert(r0.additions.map(s => s.cid.toString()).includes(r0.root.toString()))
    assert(!r0.removals.map(s => s.cid.toString()).includes(r0.root.toString()))

    const r1 = await alice.put(key0, value0)

    // nothing was added or removed
    assert.equal(r1.root.toString(), r0.root.toString())
    assert.equal(r1.additions.length, 0)
    assert.equal(r1.removals.length, 0)

    // no event should have been added to the clock
    assert.deepEqual(r1.head.map(cid => cid.toString()), r0.head.map(cid => cid.toString()))
    assert.equal(r1.event, undefined)
  })
})

class TestPail {
  /**
   * @param {Blockstore} blocks
   * @param {API.EventLink<API.Operation>[]} head
   */
  constructor (blocks, head) {
    this.blocks = blocks
    this.head = head
    /** @type {API.ShardLink?} */
    this.root = null
  }

  /** @param {API.EventLink<API.Operation>} event */
  async advance (event) {
    this.head = await advance(this.blocks, this.head, event)
    const result = await root(this.blocks, this.head)
    result.additions.forEach(a => this.blocks.putSync(a.cid, a.bytes))
    this.root = result.root
    return this.head
  }

  /**
   * @param {string} key
   * @param {API.UnknownLink} value
   */
  async put (key, value) {
    const result = await put(this.blocks, this.head, key, value)
    if (result.event) this.blocks.putSync(result.event.cid, result.event.bytes)
    result.additions.forEach(a => this.blocks.putSync(a.cid, a.bytes))
    this.head = result.head
    this.root = (await root(this.blocks, this.head)).root
    return result
  }

  /**
   * @param {string} key
   * @param {API.UnknownLink} value
   */
  async putAndVis (key, value) {
    const result = await this.put(key, value)
    /** @param {API.UnknownLink} l */
    const shortLink = l => `${String(l).slice(0, 4)}..${String(l).slice(-4)}`
    /** @param {API.PutOperation|API.DeleteOperation} o */
    const renderOp = o => `${o.type}(${o.key}${o.type === 'put' ? `${shortLink(o.value)}` : ''})`
    /** @type {(e: API.EventBlockView<API.Operation>) => string} */
    const renderNodeLabel = event => {
      if (event.value.data.type === 'put' || event.value.data.type === 'del') {
        return `${shortLink(event.cid)}\\n${renderOp(event.value.data)})`
      } else if (event.value.data.type === 'batch') {
        return `${shortLink(event.cid)}\\nbatch(${event.value.data.ops.map(renderOp)})`
      }
      // @ts-expect-error
      throw new Error(`unknown operation: ${event.value.data.type}`)
    }
    for await (const line of vis(this.blocks, result.head, { renderNodeLabel })) {
      console.log(line)
    }
    return result
  }

  /** @param {string} key */
  async get (key) {
    return get(this.blocks, this.head, key)
  }

  /**
   * @param {object} [options]
   * @param {string} [options.prefix]
   */
  async * entries (options) {
    yield * entries(this.blocks, this.head, options)
  }
}
