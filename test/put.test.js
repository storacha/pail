import { describe, it } from 'mocha'
import assert from 'node:assert'
import { nanoid } from 'nanoid'
import { ShardBlock, put, MaxKeyLength, get, encodeShardBlock } from '../src/index.js'
import { putEntry } from '../src/shard.js'
import { Blockstore, randomCID } from './helpers.js'

const maxShardSize = 1024 // tiny shard size for testing

/**
 * Fill a shard until it exceeds the size limit. Returns the entry that will
 * cause the limit to exceed.
 *
 * @param {import('../src/shard').Shard} shard
 * @param {number} size
 * @param {(i: number) => Promise<import('../src/shard').ShardValueEntry>} [mkentry]
 */
async function fillShard (shard, size, mkentry) {
  mkentry = mkentry ?? (async () => [nanoid(), await randomCID(32)])
  let i = 0
  while (true) {
    const entry = await mkentry(i)
    const blk = await encodeShardBlock(putEntry(shard, entry))
    if (blk.bytes.length > size) return { shard, entry }
    shard = putEntry(shard, entry)
    i++
  }
}

describe('put', () => {
  it('put to empty shard', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)
    const result = await put(blocks, root.cid, 'test', dataCID)

    assert.equal(result.removals.length, 1)
    assert.equal(result.removals[0].cid.toString(), root.cid.toString())
    assert.equal(result.additions.length, 1)
    assert.equal(result.additions[0].value.length, 1)
    assert.equal(result.additions[0].value[0][0], 'test')
    assert.equal(result.additions[0].value[0][1].toString(), dataCID.toString())
  })

  it('put same value to existing key', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)
    const result0 = await put(blocks, root.cid, 'test', dataCID)

    assert.equal(result0.removals.length, 1)
    assert.equal(result0.removals[0].cid.toString(), root.cid.toString())
    assert.equal(result0.additions.length, 1)
    assert.equal(result0.additions[0].value.length, 1)
    assert.equal(result0.additions[0].value[0][0], 'test')
    assert.equal(result0.additions[0].value[0][1].toString(), dataCID.toString())

    for (const b of result0.additions) {
      await blocks.put(b.cid, b.bytes)
    }

    const result1 = await put(blocks, result0.root, 'test', dataCID)

    assert.equal(result1.removals.length, 0)
    assert.equal(result1.additions.length, 0)
    assert.equal(result1.root.toString(), result0.root.toString())
  })

  it('auto-shards on long key', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)
    const key = Array(MaxKeyLength + 1).fill('a').join('')
    const result = await put(blocks, root.cid, key, dataCID)

    assert.equal(result.removals.length, 1)
    assert.equal(result.removals[0].cid.toString(), root.cid.toString())
    assert.equal(result.additions.length, 2)
    assert.equal(result.additions[0].value.length, 1)
    assert.equal(result.additions[0].value[0][0], key.slice(-1))
    assert.equal(result.additions[0].value[0][1].toString(), dataCID.toString())
    assert.equal(result.additions[1].value.length, 1)
    assert.equal(result.additions[1].value[0][0], key.slice(0, -1))
    assert.equal(result.additions[1].value[0][1][0].toString(), result.additions[0].cid.toString())
  })

  it('auto-shards on super long key', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)
    const key = Array(MaxKeyLength * 2 + 1).fill('b').join('')
    const result = await put(blocks, root.cid, key, dataCID)

    assert.equal(result.removals.length, 1)
    assert.equal(result.removals[0].cid.toString(), root.cid.toString())
    assert.equal(result.additions.length, 3)
    assert.equal(result.additions[0].value.length, 1)
    assert.equal(result.additions[0].value[0][0], key.slice(-1))
    assert.equal(result.additions[0].value[0][1].toString(), dataCID.toString())
    assert.equal(result.additions[1].value.length, 1)
    assert.equal(result.additions[1].value[0][0], key.slice(MaxKeyLength, MaxKeyLength * 2))
    assert.equal(result.additions[1].value[0][1][0].toString(), result.additions[0].cid.toString())
    assert.equal(result.additions[2].value.length, 1)
    assert.equal(result.additions[2].value[0][0], key.slice(0, MaxKeyLength))
    assert.equal(result.additions[2].value[0][1][0].toString(), result.additions[1].cid.toString())
  })

  // TODO: deep shard propagates to root

  it('shards at size limit', async () => {
    const blocks = new Blockstore()
    const pfx = 'test/'
    const { shard, entry: [k, v] } = await fillShard([], maxShardSize, async () => {
      return [pfx + nanoid(), await randomCID(1)]
    })
    const rootblk0 = await encodeShardBlock(shard)
    await blocks.put(rootblk0.cid, rootblk0.bytes)

    const { root, additions, removals } = await put(blocks, rootblk0.cid, k, v, { maxShardSize })

    assert.notEqual(root.toString(), rootblk0.cid.toString())
    assert.equal(removals.length, 1)
    assert.equal(removals[0].cid.toString(), rootblk0.cid.toString())

    for (const b of additions) {
      await blocks.put(b.cid, b.bytes)
    }

    const rootblk1 = await blocks.getShardBlock(root)

    const entry = rootblk1.value.find(([, v]) => Array.isArray(v))
    assert(entry, 'should find a shard entry')
    assert(entry[0].startsWith(pfx))

    for (const [k, v] of rootblk0.value) {
      const value = await get(blocks, rootblk1.cid, k)
      assert(value)
      assert.equal(value.toString(), v.toString())
    }
  })
})
