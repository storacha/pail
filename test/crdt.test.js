import { describe, it } from 'mocha'
import assert from 'node:assert'
import { advance, vis } from '../src/clock.js'
import { put, get, root, entries } from '../src/crdt.js'
import { Blockstore, randomCID } from './helpers.js'

describe('CRDT', () => {
  it('put a value to a new clock', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])
    const key = 'key'
    const value = await randomCID(32)
    const { event, head } = await alice.putAndVis(key, value)

    assert.equal(event.value.data.type, 'put')
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

    assert.equal(result.event.value.data.type, 'put')
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

    /** @type {Array<[string, import('../src/link').AnyLink]>} */
    const data = [
      ['banana', await randomCID(32)],
      ['kiwi', await randomCID(32)],
      ['mango', await randomCID(32)],
      ['orange', await randomCID(32)],
      ['pear', await randomCID(32)]
    ]

    const { event: aevent0 } = await alice.put(data[0][0], data[0][1])
    const { event: bevent0 } = await bob.put(data[1][0], data[1][1])

    const carol = new TestPail(blocks, bob.head)

    const { event: bevent1 } = await bob.put(data[2][0], data[2][1])
    const { event: cevent1 } = await carol.put(data[3][0], data[3][1])

    await alice.advance(cevent1.cid)
    await alice.advance(bevent0.cid)
    await alice.advance(bevent1.cid)
    await bob.advance(aevent0.cid)

    const { event: aevent1 } = await alice.putAndVis(data[4][0], data[4][1])

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

    /** @type {Array<[string, import('../src/link').AnyLink]>} */
    const data = [
      ['banana', await randomCID(32)],
      ['kiwi', await randomCID(32)]
    ]

    await alice.put(data[0][0], data[0][1])
    const { event } = await bob.put(data[1][0], data[1][1])

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

    /** @type {Array<[string, import('../src/link').AnyLink]>} */
    const data = [
      ['banana', await randomCID(32)],
      ['kiwi', await randomCID(32)]
    ]

    await alice.put(data[0][0], data[0][1])
    const { event } = await bob.put(data[1][0], data[1][1])

    await alice.advance(event.cid)

    for await (const [k, v] of alice.entries()) {
      assert(v.toString(), new Map(data).get(k)?.toString())
    }
  })

  it('root in additions', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])

    const key0 = 'key0'
    const value0 = await randomCID(32)

    const r1 = await alice.put(key0, value0)
    const root1 = r1.root.toString()
    const add1 = r1.additions.map(a => a.cid.toString())
    assert(add1.indexOf(root1) !== -1)

    const r2 = await alice.put(key0, value0)
    const root2 = r2.root.toString()
    const add2 = r2.additions.map(a => a.cid.toString())
    assert(add2.indexOf(root2) !== -1)
  })
})

class TestPail {
  /**
   * @param {Blockstore} blocks
   * @param {import('../src/clock').EventLink<import('../src/crdt').EventData>[]} head
   */
  constructor (blocks, head) {
    this.blocks = blocks
    this.head = head
    /** @type {import('../src/shard.js').ShardLink?} */
    this.root = null
  }

  /** @param {import('../src/clock').EventLink<import('../src/crdt').EventData>} event */
  async advance (event) {
    this.head = await advance(this.blocks, this.head, event)
    const result = await root(this.blocks, this.head)
    result.additions.forEach(a => this.blocks.putSync(a.cid, a.bytes))
    this.root = result.root
    return this.head
  }

  /**
   * @param {string} key
   * @param {import('../src/link').AnyLink} value
   */
  async put (key, value) {
    const result = await put(this.blocks, this.head, key, value)
    this.blocks.putSync(result.event.cid, result.event.bytes)
    result.additions.forEach(a => this.blocks.putSync(a.cid, a.bytes))
    this.head = result.head
    this.root = (await root(this.blocks, this.head)).root
    return result
  }

  /**
   * @param {string} key
   * @param {import('../src/link').AnyLink} value
   */
  async putAndVis (key, value) {
    const result = await this.put(key, value)
    /** @param {import('../src/link').AnyLink} l */
    const shortLink = l => `${String(l).slice(0, 4)}..${String(l).slice(-4)}`
    /** @type {(e: import('../src/clock').EventBlockView<import('../src/crdt').EventData>) => string} */
    const renderNodeLabel = event => {
      return event.value.data.type === 'put'
        ? `${shortLink(event.cid)}\\nput(${event.value.data.key}, ${shortLink(event.value.data.value)})`
        : `${shortLink(event.cid)}\\ndel(${event.value.data.key})`
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
