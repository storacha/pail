import { describe, it } from 'mocha'
import assert from 'node:assert'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { ShardBlock } from '../src/shard.js'
import * as Pail from '../src/index.js'
import * as Batch from '../src/batch/index.js'
import { Blockstore, randomCID, randomString } from './helpers.js'

describe('batch', () => {
  it('batches puts', async () => {
    const rootblk = await ShardBlock.create({ maxKeyLength: 4 })
    const blocks = new Blockstore()
    await blocks.put(rootblk.cid, rootblk.bytes)

    const ops = []
    for (let i = 0; i < 1000; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batcher = await Batch.create(blocks, rootblk.cid)
    for (const op of ops) {
      await batcher.put(op.key, op.value)
    }
    const { root, additions, removals } = await batcher.commit()

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
    const root = await ShardBlock.create({ maxKeyLength: 4 })
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const ops = []
    for (let i = 0; i < 1000; i++) {
      ops.push({ type: 'put', key: `test${randomString(10)}`, value: await randomCID() })
    }

    const batcher = await Batch.create(blocks, root.cid)
    for (const op of ops) {
      await batcher.put(op.key, op.value)
    }
    const { root: batchedRoot } = await batcher.commit()

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
})
