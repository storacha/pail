import fs from 'fs'
import { Readable } from 'stream'
import { CarWriter } from '@ipld/car'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
// eslint-disable-next-line no-unused-vars
import { get, entries } from '../src/index.js'
// eslint-disable-next-line no-unused-vars
import { MemoryBlockstore } from '../src/block.js'
import { ShardFetcher } from '../src/shard.js'

/**
 * @param {MemoryBlockstore} blocks
 * @param {API.ShardLink} root
 */
export const writePail = async (blocks, root) => {
  // @ts-expect-error
  const { writer, out } = CarWriter.create(root)
  const finishPromise = new Promise(resolve => {
    Readable.from(out).pipe(fs.createWriteStream('./bench.car')).on('finish', resolve)
  })

  for (const b of blocks.entries()) {
    // @ts-expect-error
    await writer.put(b)
  }
  await writer.close()
  await finishPromise
}

/**
 * @param {MemoryBlockstore} blocks
 * @param {API.ShardLink} root
 */
export const collectMetrics = async (blocks, root) => {
  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)

  let maxDepth = 0
  let totalDepth = 0
  let totalEntries = 0

  let totalShards = 1
  let maxShardSize = rshard.bytes.length
  let totalSize = rshard.bytes.length

  /**
   * @param {API.ShardEntry} entry
   * @param {number} depth
   */
  const collectData = async ([, v], depth) => {
    if (!Array.isArray(v)) {
      totalEntries++
      maxDepth = depth > maxDepth ? depth : maxDepth
      totalDepth += depth
      return
    }
    if (v[1]) totalEntries++
    const blk = await shards.get(v[0])
    totalShards++
    maxShardSize = blk.bytes.length > maxShardSize ? blk.bytes.length : maxShardSize
    totalSize += blk.bytes.length
    return Promise.all(blk.value.entries.map(e => collectData(e, depth + 1)))
  }

  for (const entry of rshard.value.entries) {
    await collectData(entry, 1)
  }

  return {
    maxDepth,
    avgDepth: Math.round(totalDepth / totalEntries),
    totalEntries,
    totalShards,
    maxShardSize,
    avgShardSize: Math.round(totalSize / totalShards),
    totalSize
  }
}
