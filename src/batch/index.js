// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher, isPrintableASCII } from '../shard.js'
import * as Shard from '../shard.js'
import * as BatcherShard from './shard.js'

/** @implements {API.Batcher} */
class Batcher {
  #committed = false

  /**
   * @param {object} init
   * @param {API.BlockFetcher} init.blocks Block storage.
   * @param {API.BatcherShardEntry[]} init.entries The entries in this shard.
   * @param {string} init.prefix Key prefix.
   * @param {number} init.version Shard compatibility version.
   * @param {string} init.keyChars Characters allowed in keys, referring to a known character set.
   * @param {number} init.maxKeySize Max key size in bytes.
   * @param {API.ShardBlockView} init.base Original shard this batcher is based on.
   */
  constructor ({ blocks, entries, prefix, version, keyChars, maxKeySize, base }) {
    this.blocks = blocks
    this.prefix = prefix
    this.entries = [...entries]
    this.base = base
    this.version = version
    this.keyChars = keyChars
    this.maxKeySize = maxKeySize
  }

  /**
   * @param {string} key The key of the value to put.
   * @param {API.UnknownLink} value The value to put.
   * @returns {Promise<void>}
   */
  async put (key, value) {
    if (this.#committed) throw new BatchCommittedError()
    return put(this.blocks, this, key, value)
  }

  async commit () {
    if (this.#committed) throw new BatchCommittedError()
    this.#committed = true
    return commit(this)
  }

  /**
   * @param {object} init
   * @param {API.BlockFetcher} init.blocks Block storage.
   * @param {API.ShardLink} init.link CID of the shard block.
   */
  static async create ({ blocks, link }) {
    const shards = new ShardFetcher(blocks)
    const base = await shards.get(link)
    return new Batcher({ blocks, base, ...base.value })
  }
}

/**
 * @param {API.BlockFetcher} blocks
 * @param {API.BatcherShard} shard
 * @param {string} key The key of the value to put.
 * @param {API.UnknownLink} value The value to put.
 * @returns {Promise<void>}
 */
export const put = async (blocks, shard, key, value) => {
  if (shard.keyChars !== Shard.KeyCharsASCII) {
    throw new Error(`unsupported key character set: ${shard.keyChars}`)
  }
  if (!isPrintableASCII(key)) {
    throw new Error('key contains non-ASCII characters')
  }
  // ensure utf8 encoded key is smaller than max
  if (new TextEncoder().encode(key).length > shard.maxKeySize) {
    throw new Error(`UTF-8 encoded key exceeds max size of ${shard.maxKeySize} bytes`)
  }

  const shards = new ShardFetcher(blocks)
  const dest = await traverse(shards, shard, key)
  if (dest.shard !== shard) {
    shard = dest.shard
    key = dest.key
  }

  /** @type {API.BatcherShardEntry} */
  let entry = [dest.key, value]
  let targetEntries = [...dest.shard.entries]

  for (const [i, e] of targetEntries.entries()) {
    const [k, v] = e

    // is this just a replace?
    if (k === dest.key) break

    // do we need to shard this entry?
    const shortest = k.length < dest.key.length ? k : dest.key
    const other = shortest === k ? dest.key : k
    let common = ''
    for (const char of shortest) {
      const next = common + char
      if (!other.startsWith(next)) break
      common = next
    }
    if (common.length) {
      /** @type {API.ShardEntry[]} */
      let entries = []

      // if the existing entry key or new key is equal to the common prefix,
      // then the existing value / new value needs to persist in the parent
      // shard. Otherwise they persist in this new shard.
      if (common !== dest.key) {
        entries = Shard.putEntry(entries, [dest.key.slice(common.length), value])
      }
      if (common !== k) {
        entries = Shard.putEntry(entries, asShardEntry([k.slice(common.length), v]))
      }

      let child = BatcherShard.create({
        ...Shard.configure(dest.shard),
        prefix: dest.shard.prefix + common,
        entries
      })
  
      // need to spread as access by index does not consider utf-16 surrogates
      const commonChars = [...common]

      // create parent shards for each character of the common prefix
      for (let i = commonChars.length - 1; i > 0; i--) {
        /** @type {API.ShardEntryShardValue | API.ShardEntryShardAndValueValue} */
        let parentValue
        // if the first iteration and the existing entry key is equal to the
        // common prefix, then existing value needs to persist in this parent
        if (i === commonChars.length - 1 && common === k) {
          if (Array.isArray(v)) throw new Error('found a shard link when expecting a value')
          parentValue = [child, v]
        } else if (i === commonChars.length - 1 && common === dest.key) {
          parentValue = [child, value]
        } else {
          parentValue = [child]
        }
        const parent = BatcherShard.create({
          ...Shard.configure(dest.shard),
          prefix: dest.shard.prefix + commonChars.slice(0, i).join(''),
          entries: [[commonChars[i], parentValue]]
        })
        child = parent
      }

      // remove the sharded entry
      targetEntries.splice(i, 1)

      // create the entry that will be added to target
      if (commonChars.length === 1 && common === k) {
        if (Array.isArray(v)) throw new Error('found a shard link when expecting a value')
        entry = [commonChars[0], [child, v]]
      } else if (commonChars.length === 1 && common === dest.key) {
        entry = [commonChars[0], [child, value]]
      } else {
        entry = [commonChars[0], [child]]
      }
      break
    }
  }

  shard.entries = Shard.putEntry(asShardEntries(targetEntries), asShardEntry(entry))
}

/**
 * Traverse from the passed shard through to the correct shard for the passed
 * key.
 *
 * @param {ShardFetcher} shards
 * @param {API.BatcherShard} shard
 * @param {string} key
 * @returns {Promise<{ shard: API.BatcherShard, key: string }>}
 */
export const traverse = async (shards, shard, key) => {
  for (let i = 0; i < shard.entries.length; i++) {
    const [k, v] = shard.entries[i]
    if (key <= k) break
    if (key.startsWith(k) && Array.isArray(v)) {
      if (Shard.isShardLink(v[0])) {
        const blk = await shards.get(v[0])
        const batcher = BatcherShard.create({ base: blk, ...blk.value })
        shard.entries[i] = [k, v[1] == null ? [batcher] : [batcher, v[1]]]
        return traverse(shards, batcher, key.slice(k.length))
      }
      return traverse(shards, v[0], key.slice(k.length))
    }
  }
  return { shard, key }
}

/**
 * Encode all altered shards in the batch and return the new root CID and
 * difference blocks.
 *
 * @param {API.BatcherShard} shard
 */
export const commit = async shard => {
  /** @type {API.ShardBlockView[]} */
  const additions = []
  /** @type {API.ShardBlockView[]} */
  const removals = []

  /** @type {API.ShardEntry[]} */
  const entries = []
  for (const entry of shard.entries) {
    if (Array.isArray(entry[1]) && !Shard.isShardLink(entry[1][0])) {
      const result = await commit(entry[1][0])
      entries.push([
        entry[0],
        entry[1][1] == null ? [result.root] : [result.root, entry[1][1]]
      ])
      additions.push(...result.additions)
      removals.push(...result.removals)
    } else {
      entries.push(asShardEntry(entry))
    }
  }

  const block = await Shard.encodeBlock(Shard.withEntries(entries, shard))
  additions.push(block)

  if (shard.base && shard.base.cid.toString() === block.cid.toString()) {
    return { root: block.cid, additions: [], removals: [] }
  }

  if (shard.base) removals.push(shard.base)

  return { root: block.cid, additions, removals }
}

/** @param {API.BatcherShardEntry[]} entries */
const asShardEntries = entries => /** @type {API.ShardEntry[]} */ (entries)

/** @param {API.BatcherShardEntry} entry */
const asShardEntry = entry => /** @type {API.ShardEntry} */ (entry)

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root shard block.
 * @returns {Promise<API.Batcher>}
 */
export const create = (blocks, root) => Batcher.create({ blocks, link: root })

export class BatchCommittedError extends Error {
  /**
   * @param {string} [message]
   * @param {ErrorOptions} [options]
   */
  constructor (message, options) {
    super(message ?? 'batch already committed', options)
    this.code = BatchCommittedError.code
  }

  static code = 'ERR_BATCH_COMMITTED'
}
