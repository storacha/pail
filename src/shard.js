import * as Link from 'multiformats/link'
import { Block, encode, decode } from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as dagCBOR from '@ipld/dag-cbor'
import { tokensToLength } from 'cborg/length'
import { Token, Type } from 'cborg'
// eslint-disable-next-line no-unused-vars
import * as API from './api.js'

export const MaxKeyLength = 64
export const MaxShardSize = 512 * 1024

const CID_TAG = new Token(Type.tag, 42)

/**
 * @extends {Block<API.Shard, typeof dagCBOR.code, typeof sha256.code, 1>}
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
  const { cid, bytes } = await encode({ value, codec: dagCBOR, hasher: sha256 })
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
  const { cid, value } = await decode({ bytes, codec: dagCBOR, hasher: sha256 })
  if (!isShard(value)) throw new Error(`invalid shard: ${cid}`)
  return new ShardBlock({ cid, value, bytes, prefix: prefix ?? '' })
}

/**
 * @param {any} value
 * @returns {value is API.Shard}
 */
export const isShard = (value) =>
  value != null &&
  typeof value === 'object' &&
  Array.isArray(value.entries) &&
  typeof value.maxSize === 'number' &&
  typeof value.maxKeyLength === 'number'

/**
 * @param {any} value
 * @returns {value is API.ShardLink}
 */
export const isShardLink = (value) =>
  Link.isLink(value) &&
  value.code === dagCBOR.code

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

  for (const [i, entry] of target.entries()) {
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
        const matches = entries.filter(entry => entry[0].startsWith(pfx))
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

/** @param {API.Shard} shard */
export const encodedLength = (shard) => {
  let entriesLength = 0
  for (const entry of shard.entries) {
    entriesLength += entryEncodedLength(entry)
  }
  const tokens = [
    new Token(Type.map, 3),
    new Token(Type.string, 'entries'),
    new Token(Type.array, shard.entries.length),
    new Token(Type.string, 'maxKeyLength'),
    new Token(Type.uint, shard.maxKeyLength),
    new Token(Type.string, 'maxSize'),
    new Token(Type.uint, shard.maxSize)
  ]
  return tokensToLength(tokens) + entriesLength
}

/** @param {API.ShardEntry} entry */
const entryEncodedLength = entry => {
  const tokens = [
    new Token(Type.array, entry.length),
    new Token(Type.string, entry[0])
  ]
  if (Array.isArray(entry[1])) {
    tokens.push(new Token(Type.array, entry[1].length))
    for (const link of entry[1]) {
      tokens.push(CID_TAG)
      tokens.push(new Token(Type.bytes, { length: link.byteLength + 1 }))
    }
  } else {
    tokens.push(CID_TAG)
    tokens.push(new Token(Type.bytes, { length: entry[1].byteLength + 1 }))
  }
  return tokensToLength(tokens)
}
