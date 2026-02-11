import { expect } from 'vitest'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { ShardBlock } from '../src/shard.js'
import * as Pail from '../src/index.js'
import * as Batch from '../src/batch/index.js'
import { Blockstore, randomCID, randomString, vis } from './helpers.js'

describe('batch', () => {
  it('batches puts', async () => {
    const rootblk = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(rootblk.cid, rootblk.bytes)

    const ops = []
    for (let i = 0; i < 1000; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batch = await Batch.create(blocks, rootblk.cid)
    for (const op of ops) {
      await batch.put(op.key, op.value)
    }
    const { root, additions, removals } = await batch.commit()

    for (const b of removals) {
      blocks.deleteSync(b.cid)
    }
    for (const b of additions) {
      blocks.putSync(b.cid, b.bytes)
    }

    assert.equal(removals.length, 1)
    assert.equal(removals[0].cid.toString(), rootblk.cid.toString())

    for (const o of ops) {
      const value = await Pail.get(blocks, root, o.key)
      assert(value)
      assert.equal(value.toString(), o.value.toString())
    }
  })

  it('create the same DAG as non-batched puts', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const ops = []
    for (let i = 0; i < 1000; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batch = await Batch.create(blocks, root.cid)
    for (const op of ops) {
      await batch.put(op.key, op.value)
    }
    const { root: batchedRoot } = await batch.commit()

    /** @type {API.ShardLink} */
    let nonBatchedRoot = root.cid
    for (const op of ops) {
      const { root, additions } = await Pail.put(blocks, nonBatchedRoot, op.key, op.value)
      nonBatchedRoot = root
      for (const b of additions) {
        blocks.putSync(b.cid, b.bytes)
      }
    }

    assert.equal(batchedRoot.toString(), nonBatchedRoot.toString())
  })

  it('error when put after commit', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const ops = []
    for (let i = 0; i < 5; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batch = await Batch.create(blocks, root.cid)
    for (const op of ops) {
      await batch.put(op.key, op.value)
    }
    await batch.commit()
    await expect(async () => batch.put('test', await randomCID())).rejects.toThrowError(/batch already committed/)
  })

  it('error when commit after commit', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const ops = []
    for (let i = 0; i < 5; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batch = await Batch.create(blocks, root.cid)
    for (const op of ops) {
      await batch.put(op.key, op.value)
    }
    await batch.commit()
    await expect(async () => batch.commit()).rejects.toThrowError(/batch already committed/)
  })

  it('batches dels', async () => {
    const rootblk = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(rootblk.cid, rootblk.bytes)

    // put some keys
    const keys = []
    for (let i = 0; i < 10; i++) {
      keys.push(`test${randomString(10)}`)
    }
    /** @type {API.ShardLink} */
    let current = rootblk.cid
    for (const key of keys) {
      const { root, additions } = await Pail.put(blocks, current, key, await randomCID())
      current = root
      for (const b of additions) blocks.putSync(b.cid, b.bytes)
    }

    // batch-delete the first 5
    const batch = await Batch.create(blocks, current)
    for (const key of keys.slice(0, 5)) {
      await batch.del(key)
    }
    const { root, additions, removals } = await batch.commit()

    for (const b of removals) blocks.deleteSync(b.cid)
    for (const b of additions) blocks.putSync(b.cid, b.bytes)

    // deleted keys should be gone
    for (const key of keys.slice(0, 5)) {
      const value = await Pail.get(blocks, root, key)
      assert.equal(value, undefined)
    }

    // remaining keys should still exist
    for (const key of keys.slice(5)) {
      const value = await Pail.get(blocks, root, key)
      assert(value)
    }
  })

  it('mixed put and del in same batch', async () => {
    const rootblk = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(rootblk.cid, rootblk.bytes)

    // put initial keys
    /** @type {API.ShardLink} */
    let current = rootblk.cid
    const value0 = await randomCID()
    const res0 = await Pail.put(blocks, current, 'apple', value0)
    current = res0.root
    for (const b of res0.additions) blocks.putSync(b.cid, b.bytes)

    const res1 = await Pail.put(blocks, current, 'banana', await randomCID())
    current = res1.root
    for (const b of res1.additions) blocks.putSync(b.cid, b.bytes)

    // batch: delete apple, put cherry
    const cherryValue = await randomCID()
    const batch = await Batch.create(blocks, current)
    await batch.del('apple')
    await batch.put('cherry', cherryValue)
    const { root, additions, removals } = await batch.commit()

    for (const b of removals) blocks.deleteSync(b.cid)
    for (const b of additions) blocks.putSync(b.cid, b.bytes)

    assert.equal(await Pail.get(blocks, root, 'apple'), undefined)
    assert(await Pail.get(blocks, root, 'banana'))
    const cherry = await Pail.get(blocks, root, 'cherry')
    assert(cherry)
    assert.equal(cherry.toString(), cherryValue.toString())
  })

  it('del non-existent key is a no-op', async () => {
    const rootblk = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(rootblk.cid, rootblk.bytes)

    const { root: r0, additions: a0 } = await Pail.put(blocks, rootblk.cid, 'apple', await randomCID())
    for (const b of a0) blocks.putSync(b.cid, b.bytes)

    const batch = await Batch.create(blocks, r0)
    await batch.del('nonexistent')
    const { root } = await batch.commit()

    // root should be unchanged
    assert.equal(root.toString(), r0.toString())
  })

  it('error when del after commit', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const batch = await Batch.create(blocks, root.cid)
    await batch.put('test', await randomCID())
    await batch.commit()
    await expect(async () => batch.del('test')).rejects.toThrowError(/batch already committed/)
  })

  it('traverses existing shards to put values', async () => {
    const rootblk = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(rootblk.cid, rootblk.bytes)

    const ops0 = []
    for (let i = 0; i < 5; i++) {
      ops0.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }
    ops0.push({ type: 'put', key: 'test', value: await randomCID() })

    const ops1 = []
    for (const op of ops0) {
      ops1.push({ ...op, value: await randomCID() })
    }

    const batch0 = await Batch.create(blocks, rootblk.cid)
    for (const op of ops0) {
      await batch0.put(op.key, op.value)
    }
    const { root: root0, additions: additions0 } = await batch0.commit()
    for (const b of additions0) {
      blocks.putSync(b.cid, b.bytes)
    }

    const batch1 = await Batch.create(blocks, root0)
    for (const op of ops1) {
      await batch1.put(op.key, op.value)
    }
    const { root: root1, additions: additions1 } = await batch1.commit()
    for (const b of additions1) {
      blocks.putSync(b.cid, b.bytes)
    }

    vis(blocks, root1)

    for (const o of ops1) {
      const value = await Pail.get(blocks, root1, o.key)
      assert(value)
      assert.equal(value.toString(), o.value.toString())
    }
  })
})
