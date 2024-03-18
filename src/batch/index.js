// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher } from '../shard.js'
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
  const shards = new ShardFetcher(blocks)
  const dest = await traverse(shards, key, shard)
  if (dest.shard !== shard) {
    shard = dest.shard
    key = dest.key
  }

  /** @type {API.BatcherShardEntry} */
  let entry = [key, value]
  /** @type {API.BatcherShard|undefined} */
  let batcher

  // if the key in this shard is longer than allowed, then we need to make some
  // intermediate shards.
  if (key.length > shard.maxKeyLength) {
    const pfxskeys = Array.from(Array(Math.ceil(key.length / shard.maxKeyLength)), (_, i) => {
      const start = i * shard.maxKeyLength
      return {
        prefix: shard.prefix + key.slice(0, start),
        key: key.slice(start, start + shard.maxKeyLength)
      }
    })

    entry = [pfxskeys[pfxskeys.length - 1].key, value]
    batcher = BatcherShard.create({
      entries: [entry],
      prefix: pfxskeys[pfxskeys.length - 1].prefix,
      ...Shard.configure(shard)
    })

    for (let i = pfxskeys.length - 2; i > 0; i--) {
      entry = [pfxskeys[i].key, [batcher]]
      batcher = BatcherShard.create({
        entries: [entry],
        prefix: pfxskeys[i].prefix,
        ...Shard.configure(shard)
      })
    }

    entry = [pfxskeys[0].key, [batcher]]
  }

  shard.entries = Shard.putEntry(asShardEntries(shard.entries), asShardEntry(entry))

  // TODO: adjust size automatically
  const size = BatcherShard.encodedLength(shard)
  if (size > shard.maxSize) {
    const common = Shard.findCommonPrefix(
      asShardEntries(shard.entries),
      entry[0]
    )
    if (!common) throw new Error('shard limit reached')
    const { prefix } = common
    /** @type {API.BatcherShardEntry[]} */
    const matches = common.matches

    const entries = matches
      .filter(m => m[0] !== prefix)
      .map(m => {
        m = [...m]
        m[0] = m[0].slice(prefix.length)
        return m
      })

    const batcher = BatcherShard.create({
      entries,
      prefix: shard.prefix + prefix,
      ...Shard.configure(shard)
    })

    /** @type {API.ShardEntryShardValue | API.ShardEntryShardAndValueValue} */
    let value
    const pfxmatch = matches.find(m => m[0] === prefix)
    if (pfxmatch) {
      if (Array.isArray(pfxmatch[1])) {
        // should not happen! all entries with this prefix should have been
        // placed within this shard already.
        throw new Error(`expected "${prefix}" to be a shard value but found a shard link`)
      }
      value = [batcher, pfxmatch[1]]
    } else {
      value = [batcher]
    }

    shard.entries = Shard.putEntry(
      asShardEntries(shard.entries.filter(e => matches.every(m => e[0] !== m[0]))),
      asShardEntry([prefix, value])
    )
  }
}

/**
 * Traverse from the passed shard through to the correct shard for the passed
 * key.
 *
 * @param {ShardFetcher} shards
 * @param {string} key
 * @param {API.BatcherShard} shard
 * @returns {Promise<{ shard: API.BatcherShard, key: string }>}
 */
export const traverse = async (shards, key, shard) => {
  for (let i = 0; i < shard.entries.length; i++) {
    const [k, v] = shard.entries[i]
    if (key <= k) break
    if (key.startsWith(k) && Array.isArray(v)) {
      if (Shard.isShardLink(v[0])) {
        const blk = await shards.get(v[0])
        const batcher = BatcherShard.create({ base: blk, ...blk.value })
        shard.entries[i] = [k, v[1] == null ? [batcher] : [batcher, v[1]]]
        return traverse(shards, key.slice(k.length), batcher)
      }
      return traverse(shards, key.slice(k.length), v[0])
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
