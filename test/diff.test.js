import assert from 'node:assert'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { put } from '../src/index.js'
import { ShardBlock } from '../src/shard.js'
import { difference } from '../src/diff.js'
import { Blockstore, randomCID } from './helpers.js'

describe('diff', () => {
  it('diffs a non-sharded addition', async () => {
    const empty = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(empty.cid, empty.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const testdata = [
      ['a', await randomCID(32)]
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

    const diff = await difference(blocks, empty.cid, root)

    assert.equal(diff.shards.removals.length, 1)
    assert.equal(diff.shards.removals[0].cid.toString(), empty.cid.toString())
    assert.equal(diff.shards.additions.length, 1)
    assert.equal(diff.shards.additions[0].cid.toString(), root.toString())

    assert.equal(diff.keys.length, testdata.length)
    for (const [k, v] of testdata) {
      const d = diff.keys.find(p => p[0] === k)
      assert(d)
      assert.equal(d[1][0], null)
      assert.equal(d[1][1]?.toString(), v.toString())
    }
  })
})
