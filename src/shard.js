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

  /** @param {{ maxSize?: number, maxKeyLength?: number }} [config] */
  static create (config) {
    return encodeBlock(create(config))
  }
}

/**
 * @param {{ maxSize?: number, maxKeyLength?: number }} [config]
 * @returns {API.Shard}
 */
export const create = (config) => ({
  entries: [],
  maxSize: config?.maxSize ?? MaxShardSize,
  maxKeyLength: config?.maxKeyLength ?? MaxKeyLength
})

/**
 * @param {API.ShardEntry[]} entries
 * @param {{ maxSize?: number, maxKeyLength?: number }} [config]
 * @returns {API.Shard}
 */
export const withEntries = (entries, config) => ({ ...create(config), entries })

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
 * @param {API.Shard} target Shard to put to.
 * @param {API.ShardEntry} entry
 * @returns {API.Shard}
 */
export const putEntry = (target, entry) => {
  /** @type {API.Shard} */
  const shard = create(target)

  for (const [i, [k, v]] of target.entries.entries()) {
    if (entry[0] === k) {
      // if new value is link to shard...
      if (Array.isArray(entry[1])) {
        // and old value is link to shard
        // and old value is _also_ link to data
        // and new value does not have link to data
        // then preserve old data
        if (Array.isArray(v) && v[1] != null && entry[1][1] == null) {
          shard.entries.push([k, [entry[1][0], v[1]]])
        } else {
          shard.entries.push(entry)
        }
      } else {
        // shard as well as value?
        /** @type {API.ShardEntry} */
        const newEntry = Array.isArray(v) ? [k, [v[0], entry[1]]] : entry
        shard.entries.push(newEntry)
      }
      for (let j = i + 1; j < target.entries.length; j++) {
        shard.entries.push(target.entries[j])
      }
      return shard
    }
    if (i === 0 && entry[0] < k) {
      shard.entries.push(entry)
      for (let j = i; j < target.entries.length; j++) {
        shard.entries.push(target.entries[j])
      }
      return shard
    }
    if (i > 0 && entry[0] > target.entries[i - 1][0] && entry[0] < k) {
      shard.entries.push(entry)
      for (let j = i; j < target.entries.length; j++) {
        shard.entries.push(target.entries[j])
      }
      return shard
    }
    shard.entries.push([k, v])
  }

  shard.entries.push(entry)
  return shard
}

/**
 * @param {API.Shard} shard
 * @param {string} skey Shard key to use as a base.
 */
export const findCommonPrefix = (shard, skey) => {
  const startidx = shard.entries.findIndex(([k]) => skey === k)
  if (startidx === -1) throw new Error(`key not found in shard: ${skey}`)
  let i = startidx
  /** @type {string} */
  let pfx
  while (true) {
    pfx = shard.entries[i][0].slice(0, -1)
    if (pfx.length) {
      while (true) {
        const matches = shard.entries.filter(entry => entry[0].startsWith(pfx))
        if (matches.length > 1) return { prefix: pfx, matches }
        pfx = pfx.slice(0, -1)
        if (!pfx.length) break
      }
    }
    i++
    if (i >= shard.entries.length) {
      i = 0
    }
    if (i === startidx) {
      return
    }
  }
}
