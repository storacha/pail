import * as Link from 'multiformats/link'
import { Block, encode, decode } from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as dagCBOR from '@ipld/dag-cbor'
// eslint-disable-next-line no-unused-vars
import * as API from './api.js'

export const KeyCharsASCII = 'ascii'
export const MaxKeySize = 4096

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
   */
  constructor ({ cid, value, bytes }) {
    // @ts-expect-error
    super({ cid, value, bytes })
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
  version: 1,
  keyChars: options?.keyChars ?? KeyCharsASCII,
  maxKeySize: options?.maxKeySize ?? MaxKeySize,
  prefix: options?.prefix ?? ''
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
 * @returns {Promise<API.ShardBlockView>}
 */
export const encodeBlock = async value => {
  const { cid, bytes } = await encode({ value, codec: dagCBOR, hasher: sha256 })
  const block = new ShardBlock({ cid, value, bytes })
  decodeCache.set(block.bytes, block)
  return block
}

/**
 * @param {Uint8Array} bytes
 * @returns {Promise<API.ShardBlockView>}
 */
export const decodeBlock = async bytes => {
  const block = decodeCache.get(bytes)
  if (block) return block
  const { cid, value } = await decode({ bytes, codec: dagCBOR, hasher: sha256 })
  if (!isShard(value)) throw new Error(`invalid shard: ${cid}`)
  return new ShardBlock({ cid, value, bytes })
}

/**
 * @param {any} value
 * @returns {value is API.Shard}
 */
export const isShard = value =>
  value != null &&
  typeof value === 'object' &&
  Array.isArray(value.entries) &&
  value.version === 1 &&
  typeof value.maxKeySize === 'number' &&
  typeof value.keyChars === 'string' &&
  typeof value.prefix === 'string'

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
   * @returns {Promise<API.ShardBlockView>}
   */
  async get (link) {
    const block = await this._blocks.get(link)
    if (!block) throw new Error(`missing block: ${link}`)
    return decodeBlock(block.bytes)
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
          entries.push([k, [newEntry[1][0], v[1]]])
        } else {
          entries.push(newEntry)
        }
      } else {
        // shard as well as value?
        if (Array.isArray(v)) {
          entries.push([k, [v[0], newEntry[1]]])
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
