import { expect } from 'vitest'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/crdt/api.js'
import { advance, vis } from '../src/clock/index.js'
import { put, get, root, entries } from '../src/crdt/index.js'
import * as Batch from '../src/crdt/batch/index.js'
import { Blockstore, clockVis, randomCID, randomString } from './helpers.js'

describe('CRDT', () => {
  it('put a value to a new clock', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])
    const key = 'key'
    const value = await randomCID(32)
    const { event, head } = await alice.put(key, value)
    await alice.vis()

    assert(event)
    assert(event?.value.data.type === 'put')
    assert.equal(event?.value.data.key, key)
    assert.equal(event?.value.data.value.toString(), value.toString())
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
    const result = await alice.put(key1, value1)
    await alice.vis()

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

    const { event: aevent1 } = await alice.put(data[4][0], data[4][1])
    await alice.vis()

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

describe('CRDT batch', () => {
  it('error when put after commit', async () => {
    const blocks = new Blockstore()

    const ops = []
    for (let i = 0; i < 5; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batch = await Batch.create(blocks, [])
    for (const op of ops) {
      await batch.put(op.key, op.value)
    }
    await batch.commit()
    await expect(async () => batch.put('test', await randomCID())).rejects.toThrow(/batch already committed/)
  })

  it('error when commit after commit', async () => {
    const blocks = new Blockstore()

    const ops = []
    for (let i = 0; i < 5; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batch = await Batch.create(blocks, [])
    for (const op of ops) {
      await batch.put(op.key, op.value)
    }
    await batch.commit()
    await expect(async () => batch.commit()).rejects.toThrow(/batch already committed/)
  })

  it('linear put with batch', async () => {
    const blocks = new Blockstore()
    const alice = new TestPail(blocks, [])

    const key0 = 'test0'
    const value0 = await randomCID(32)
    await alice.put(key0, value0)

    const ops = []
    for (let i = 0; i < 25; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    await alice.putBatch(ops)

    // put a new value for the first batch key
    const key1 = ops[0].key
    const value1 = await randomCID(32)
    await alice.put(key1, value1)

    await alice.vis()

    const res0 = await alice.get(key0)
    assert(res0)
    assert.equal(res0.toString(), value0.toString())

    for (const op of ops.slice(1)) {
      const res = await alice.get(op.key)
      assert(res)
      assert.equal(res.toString(), op.value.toString())
    }

    const res1 = await alice.get(key1)
    assert(res1)
    assert.equal(res1.toString(), value1.toString())
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
   * @param {Array<{ key: string, value: API.UnknownLink }>} items
   */
  async putBatch (items) {
    const batch = await Batch.create(this.blocks, this.head)
    for (const { key, value } of items) {
      await batch.put(key, value)
    }
    const result = await batch.commit()
    if (result.event) this.blocks.putSync(result.event.cid, result.event.bytes)
    result.additions.forEach(a => this.blocks.putSync(a.cid, a.bytes))
    this.head = result.head
    this.root = (await root(this.blocks, this.head)).root
    return result
  }

  async vis () {
    /** @param {API.UnknownLink} l */
    const shortLink = l => `${String(l).slice(0, 4)}..${String(l).slice(-4)}`
    /**
     * @param {API.PutOperation|API.DeleteOperation|API.BatchOperation} o
     * @returns {string}
     **/
    const renderOp = o => o.type === 'batch'
      ? `${o.ops.slice(0, 10).map(renderOp).join('\\n')}${o.ops.length > 10 ? `\\n...${o.ops.length - 10} more` : ''}`
      : `${o.type}(${o.key}${o.type === 'put'
        ? `, ${shortLink(o.value)}`
        : ''})`
    /** @type {(e: API.EventBlockView<API.Operation>) => string} */
    const renderNodeLabel = event => `${shortLink(event.cid)}\\n${renderOp(event.value.data)}`
    await clockVis(this.blocks, this.head, { renderNodeLabel })
  }

  /** @param {string} key */
  async get (key) {
    return get(this.blocks, this.head, key)
  }

  /** @param {API.EntriesOptions} [options] */
  async * entries (options) {
    yield * entries(this.blocks, this.head, options)
  }
}
