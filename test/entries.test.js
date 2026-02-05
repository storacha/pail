// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { put, entries } from '../src/index.js'
import { ShardBlock } from '../src/shard.js'
import { Blockstore, randomCID } from './helpers.js'

describe('entries', () => {
  it('lists entries in lexicographical order', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['c', await randomCID(32)],
      ['d', await randomCID(32)],
      ['a', await randomCID(32)],
      ['b', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const results = []
    for await (const entry of entries(blocks, root)) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by prefix', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const prefix = 'd'
    const results = []
    for await (const entry of entries(blocks, root, { prefix })) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).filter(k => k.startsWith(prefix)).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by key greater than string', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const gt = 'beee'
    const results = []
    for await (const entry of entries(blocks, root, { gt })) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).filter(k => k > gt).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by key greater than or equal to string', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const gte = 'beee'
    const results = []
    for await (const entry of entries(blocks, root, { gte })) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).filter(k => k >= gte).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by key less than string', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const lt = 'doo'
    const results = []
    for await (const entry of entries(blocks, root, { lt })) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).filter(k => k < lt).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by key less than or equal to string', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const lte = 'dooo'
    const results = []
    for await (const entry of entries(blocks, root, { lte })) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).filter(k => k <= lte).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by key greater than and less than or equal to string', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const gt = 'c'
    const lte = 'deee'
    const results = []
    for await (const entry of entries(blocks, root, { gt, lte })) {
      results.push(entry)
    }

    for (const [i, key] of testdata.map(d => d[0]).filter(k => k > gt && k <= lte).sort().entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries in ascending order (explicit)', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['c', await randomCID(32)],
      ['d', await randomCID(32)],
      ['a', await randomCID(32)],
      ['b', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const results = []
    for await (const entry of entries(blocks, root, { order: 'asc' })) {
      results.push(entry)
    }

    const sortedKeys = testdata.map(d => d[0]).sort()
    for (const [i, key] of sortedKeys.entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries in descending order', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['c', await randomCID(32)],
      ['d', await randomCID(32)],
      ['a', await randomCID(32)],
      ['b', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const results = []
    for await (const entry of entries(blocks, root, { order: 'desc' })) {
      results.push(entry)
    }

    const sortedKeys = testdata.map(d => d[0]).sort().reverse()
    for (const [i, key] of sortedKeys.entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by prefix in descending order', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['dccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['daaa', await randomCID(32)],
      ['beee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const prefix = 'd'
    const results = []
    for await (const entry of entries(blocks, root, { prefix, order: 'desc' })) {
      results.push(entry)
    }

    const filteredAndSorted = testdata.map(d => d[0]).filter(k => k.startsWith(prefix)).sort().reverse()
    for (const [i, key] of filteredAndSorted.entries()) {
      assert.equal(results[i][0], key)
    }
  })

  it('lists entries by range in descending order', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['aaaa', await randomCID(32)],
      ['cccc', await randomCID(32)],
      ['deee', await randomCID(32)],
      ['dooo', await randomCID(32)],
      ['eeee', await randomCID(32)]
    ]

    /** @type {API.ShardLink} */
    let root = empty.cid
    for (const [k, v] of testdata) {
      const res = await put(blocks, root, k, v)
      for (const b of res.additions) {
        await blocks.put(b.cid, b.bytes)
      }
      root = res.root
    }

    const gte = 'c'
    const lt = 'e'
    const results = []
    for await (const entry of entries(blocks, root, { gte, lt, order: 'desc' })) {
      results.push(entry)
    }

    const filteredAndSorted = testdata.map(d => d[0]).filter(k => k >= gte && k < lt).sort().reverse()
    for (const [i, key] of filteredAndSorted.entries()) {
      assert.equal(results[i][0], key)
    }
  })
})
