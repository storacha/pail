import { describe, it } from 'mocha'
import assert from 'node:assert'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { ShardBlock } from '../src/shard.js'
import * as Pail from '../src/index.js'
import * as Batch from '../src/batch/index.js'
import { Blockstore, randomCID, randomString, vis } from './helpers.js'

describe('batch', () => {
  it('batches puts (shard on key length)', async () => {
    const rootblk = await ShardBlock.create({ maxKeyLength: 4 })
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

  it('batches puts (shard on max size)', async () => {
    const rootblk = await ShardBlock.create({ maxSize: 1000 })
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

    // vis(blocks, root)
  })

  it('create the same DAG as non-batched puts', async () => {
    const root = await ShardBlock.create({ maxKeyLength: 4 })
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
    const root = await ShardBlock.create({ maxKeyLength: 4 })
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
    await assert.rejects(batch.put('test', await randomCID()), /batch already committed/)
  })

  it('error when commit after commit', async () => {
    const root = await ShardBlock.create({ maxKeyLength: 4 })
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
    await assert.rejects(batch.commit(), /batch already committed/)
  })

  it('traverses existing shards to put values', async () => {
    const rootblk = await ShardBlock.create({ maxKeyLength: 4 })
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
