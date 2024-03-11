import fs from 'fs'
import { Readable } from 'stream'
import { CarWriter } from '@ipld/car'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/v1/api.js'
import { get, entries } from '../src/v1/index.js'
import { MemoryBlockstore } from '../src/block.js'
import { ShardFetcher } from '../src/v1/shard.js'

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

/**
 * @param {MemoryBlockstore} blocks
 * @param {API.ShardLink} root
 * @param {Map<string, API.UnknownLink>} data
 */
export const verify = async (blocks, root, data) => {
  for (const [k, v] of data) {
    const result = await get(blocks, root, k)
    if (!result) throw new Error(`missing item: "${k}": ${v}`)
    if (result.toString() !== v.toString()) throw new Error(`incorrect value for ${k}: ${result} !== ${v}`)
  }
  let total = 0
  for await (const _ of entries(blocks, root)) total++
  if (data.size !== total) throw new Error(`incorrect entry count: ${total} !== ${data.size}`)
  console.log(`✅ verified correct`)
}
