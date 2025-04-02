import * as Link from 'multiformats/link'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import clc from 'cli-color'
import archy from 'archy'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { ShardFetcher, decodeBlock } from '../src/shard.js'
import { MemoryBlockstore } from '../src/block.js'
import { entries, get, put } from '../src/index.js'

/**
 * @param {number} min
 * @param {number} max
 */
export function randomInteger (min, max) {
  min = Math.ceil(min)
  max = Math.floor(max)
  return Math.floor(Math.random() * (max - min) + min)
}

const Alphabet = 'abcdefghijklmnopqrstuvwxyz-/_'

/**
 * @param {number} size
 */
export function randomString (size, alphabet = Alphabet) {
  let str = ''
  while (str.length < size) {
    str += alphabet[randomInteger(0, alphabet.length)]
  }
  return str
}

/** @param {number} [size] Number of random bytes to hash. */
export async function randomCID (size = 32) {
  const hash = await sha256.digest(await randomBytes(size))
  return Link.create(raw.code, hash)
}

/** @param {number} size */
export async function randomBytes (size) {
  const bytes = new Uint8Array(size)
  while (size) {
    const chunk = new Uint8Array(Math.min(size, 65_536))
    crypto.getRandomValues(chunk)
    size -= bytes.length
    bytes.set(chunk, size)
  }
  return bytes
}

export class Blockstore extends MemoryBlockstore {
  /**
   * @param {import('../src/api.js').ShardLink} cid
   */
  async getShardBlock (cid) {
    const blk = await this.get(cid)
    assert(blk)
    return decodeBlock(blk.bytes)
  }
}

/**
 * @param {API.BlockFetcher} blocks Block storage.
 * @param {API.ShardLink} root
 */
export const vis = async (blocks, root) => {
  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)

  /** @type {archy.Data} */
  const archyRoot = { label: `Shard(${clc.yellow(rshard.cid.toString())}) ${rshard.bytes.length + 'b'}`, nodes: [] }

  /** @param {API.ShardEntry} entry */
  const getData = async ([k, v]) => {
    if (!Array.isArray(v)) {
      return { label: `Key(${clc.magenta(k)})`, nodes: [{ label: `Value(${clc.cyan(v)})` }] }
    }
    /** @type {archy.Data} */
    const data = { label: `Key(${clc.magenta(k)})`, nodes: [] }
    if (v[1]) data.nodes?.push({ label: `Value(${clc.cyan(v[1])})` })
    const blk = await shards.get(v[0])
    data.nodes?.push({
      label: `Shard(${clc.yellow(v[0])}) ${blk.bytes.length + 'b'}`,
      nodes: await Promise.all(blk.value.entries.map(e => getData(e)))
    })
    return data
  }

  for (const entry of rshard.value.entries) {
    archyRoot.nodes?.push(await getData(entry))
  }

  console.log(archy(archyRoot))
}

/**
 * @param {API.BlockFetcher} blocks
 * @param {API.ShardLink} root
 */
export const materialize = async (blocks, root) => {
  const shards = new ShardFetcher(blocks)
  const shard = await shards.get(root)
  /** @type {any[]} */
  const entries = []
  for (const e of shard.value.entries) {
    if (Array.isArray(e[1])) {
      const v = [...e[1]]
      // @ts-expect-error
      v[0] = await materialize(blocks, e[1][0])
      entries.push([e[0], v])
    } else {
      entries.push([...e])
    }
  }
  return entries
}

/**
 * @param {Blockstore} blocks
 * @param {API.ShardLink} root
 * @param {Array<[string, API.UnknownLink]>} items
 */
export const putAll = async (blocks, root, items) => {
  for (const [k, v] of items) {
    const res = await put(blocks, root, k, v)
    for (const b of res.additions) {
      blocks.putSync(b.cid, b.bytes)
    }
    for (const b of res.removals) {
      blocks.deleteSync(b.cid)
    }
    root = res.root
  }
  return { root }
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
  // eslint-disable-next-line no-unused-vars
  for await (const _ of entries(blocks, root)) total++
  if (data.size !== total) throw new Error(`incorrect entry count: ${total} !== ${data.size}`)
}
