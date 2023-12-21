import { Block, encode, decode } from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as cbor from '@ipld/dag-cbor'
// eslint-disable-next-line no-unused-vars
import * as API from './api.js'

export const MaxKeyLength = 64
export const MaxShardSize = 512 * 1024

/**
 * @extends {Block<API.Shard, typeof cbor.code, typeof sha256.code, 1>}
 * @implements {API.ShardBlockView}
 */
export class ShardBlock extends Block {
  /**
   * @param {object} config
   * @param {API.ShardLink} config.cid
   * @param {API.Shard} config.value
   * @param {Uint8Array} config.bytes
   * @param {string} config.prefix
   */
  constructor ({ cid, value, bytes, prefix }) {
    // @ts-expect-error
    super({ cid, value, bytes })
    this.prefix = prefix
  }

  /** @param {API.ShardOptions} [options] */
  static create (options) {
    return encodeBlock(create(options))
  }
}

/**
 * @param {API.ShardOptions} [options]
 * @returns {API.Shard}
 */
export const create = (options) => ({ entries: [], ...configure(options) })

/**
 * @param {API.ShardOptions} [options]
 * @returns {API.ShardConfig}
 */
export const configure = (options) => ({
  maxSize: options?.maxSize ?? MaxShardSize,
  maxKeyLength: options?.maxKeyLength ?? MaxKeyLength
})

/**
 * @param {API.ShardEntry[]} entries
 * @param {API.ShardOptions} [options]
 * @returns {API.Shard}
 */
export const withEntries = (entries, options) => ({ ...create(options), entries })

/** @type {WeakMap<Uint8Array, API.ShardBlockView>} */
const decodeCache = new WeakMap()

/**
 * @param {API.Shard} value
 * @param {string} [prefix]
 * @returns {Promise<API.ShardBlockView>}
 */
export const encodeBlock = async (value, prefix) => {
  const { cid, bytes } = await encode({ value, codec: cbor, hasher: sha256 })
  const block = new ShardBlock({ cid, value, bytes, prefix: prefix ?? '' })
  decodeCache.set(block.bytes, block)
  return block
}

/**
 * @param {Uint8Array} bytes
 * @param {string} [prefix]
 * @returns {Promise<API.ShardBlockView>}
 */
export const decodeBlock = async (bytes, prefix) => {
  const block = decodeCache.get(bytes)
  if (block) return block
  const { cid, value } = await decode({ bytes, codec: cbor, hasher: sha256 })
  if (!isShard(value)) throw new Error(`invalid shard: ${cid}`)
  return new ShardBlock({ cid, value, bytes, prefix: prefix ?? '' })
}

/**
 * @param {any} value
 * @returns {value is API.Shard}
 */
const isShard = (value) =>
  value != null &&
  typeof value === 'object' &&
  Array.isArray(value.entries) &&
  typeof value.maxSize === 'number' &&
  typeof value.maxKeyLength === 'number'

export class ShardFetcher {
  /** @param {API.BlockFetcher} blocks */
  constructor (blocks) {
    this._blocks = blocks
  }

  /**
   * @param {API.ShardLink} link
   * @param {string} [prefix]
   * @returns {Promise<API.ShardBlockView>}
   */
  async get (link, prefix = '') {
    const block = await this._blocks.get(link)
    if (!block) throw new Error(`missing block: ${link}`)
    return decodeBlock(block.bytes, prefix)
  }
}

/**
 * @param {API.ShardEntry[]} target Entries to insert into.
 * @param {API.ShardEntry} newEntry
 * @returns {API.ShardEntry[]}
 */
export const putEntry = (target, newEntry) => {
  /** @type {API.ShardEntry[]} */
  const entries = []

  for (let [i, entry] of target.entries()) {
    const [k, v] = entry
    if (newEntry[0] === k) {
      // if new value is link to shard...
      if (Array.isArray(newEntry[1])) {
        // and old value is link to shard
        // and old value is _also_ link to data
        // and new value does not have link to data
        // then preserve old data
        if (Array.isArray(v) && v[1] != null && newEntry[1][1] == null) {          
          entries.push(Object.assign([], entry, newEntry, { 1: [newEntry[1][0], v[1]] }))
        } else {
          entries.push(newEntry)
        }
      } else {
        // shard as well as value?
        if (Array.isArray(v)) {
          entries.push(Object.assign([], entry, newEntry, { 1: [v[0], newEntry[1]] }))
        } else {
          entries.push(newEntry)
        }
      }
      for (let j = i + 1; j < target.length; j++) {
        entries.push(target[j])
      }
      return entries
    }
    if (i === 0 && newEntry[0] < k) {
      entries.push(newEntry)
      for (let j = i; j < target.length; j++) {
        entries.push(target[j])
      }
      return entries
    }
    if (i > 0 && newEntry[0] > target[i - 1][0] && newEntry[0] < k) {
      entries.push(newEntry)
      for (let j = i; j < target.length; j++) {
        entries.push(target[j])
      }
      return entries
    }
    entries.push(entry)
  }

  entries.push(newEntry)
  return entries
}

/**
 * @param {API.ShardEntry[]} entries
 * @param {string} skey Shard key to use as a base.
 */
export const findCommonPrefix = (entries, skey) => {
  const startidx = entries.findIndex(([k]) => skey === k)
  if (startidx === -1) throw new Error(`key not found in shard: ${skey}`)
  let i = startidx
  /** @type {string} */
  let pfx
  while (true) {
    pfx = entries[i][0].slice(0, -1)
    if (pfx.length) {
      while (true) {
        const matches = []
        for (let j = i - 1; j >= 0; j--) {
          if (entries[j][0].startsWith(pfx)) {
            matches.push(entries[j])
          } else {
            break
          }
        }
        if (matches.length > 1) return { prefix: pfx, matches }
        pfx = pfx.slice(0, -1)
        if (!pfx.length) break
      }
    }
    i++
    if (i >= entries.length) {
      i = 0
    }
    if (i === startidx) {
      return
    }
  }
}
