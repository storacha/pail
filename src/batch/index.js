import * as Link from 'multiformats/link'
// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher } from '../shard.js'
import * as Shard from '../shard.js'

/**
 * @template {API.BatcherShard} T
 * @implements {API.Batcher}
 */
class Batcher {
  /**
   * @param {object} arg
   * @param {ShardFetcher} arg.shards Shard storage.
   * @param {API.BatcherShardEntry<T>[]} arg.entries The entries in this shard.
   * @param {string} arg.prefix Key prefix.
   * @param {API.ShardConfig} arg.config Shard config.
   * @param {API.ShardBlockView} [arg.base] Original shard this batcher is based on.
   */
  constructor ({ shards, entries, prefix, config, base }) {
    this.shards = shards
    this.prefix = prefix
    this.entries = entries
    this.base = base
    this.maxSize = config.maxSize
    this.maxKeyLength = config.maxKeyLength
  }

  /**
   * @param {string} key The key of the value to put.
   * @param {API.UnknownLink} value The value to put.
   * @returns {Promise<void>}
   */
  async put (key, value) {
    return put({
      shards: this.shards,
      shard: this,
      key,
      value,
      createShard: arg => new Batcher(arg),
      fetchShard: Batcher.create
    })
  }

  async commit () {
    return commit(this)
  }

  /**
   * @param {object} arg
   * @param {ShardFetcher} arg.shards Shard storage.
   * @param {API.ShardLink} arg.link CID of the shard block.
   * @param {string} arg.prefix
   */
  static async create ({ shards, link, prefix }) {
    const base = await shards.get(link)
    return new Batcher({
      shards,
      entries: asBatcherShardEntries(base.value.entries),
      prefix,
      config: Shard.configure(base.value),
      base
    })
  }
}

/**
 * @template {API.BatcherShard} T
 * @param {object} arg
 * @param {ShardFetcher} arg.shards
 * @param {API.BatcherShard} arg.shard
 * @param {string} arg.key The key of the value to put.
 * @param {API.UnknownLink} arg.value The value to put.
 * @param {(arg: { shards: ShardFetcher, entries: API.BatcherShardEntry<T>[], prefix: string, config: API.ShardConfig }) => T} arg.createShard
 * @param {(arg: { shards: ShardFetcher, link: API.ShardLink, prefix: string }) => Promise<T>} arg.fetchShard
 * @returns {Promise<void>}
 */
export const put = async ({ shards, shard, key, value, createShard, fetchShard }) => {
  const dest = await traverse({ shards, key, shard, fetchShard })
  if (dest.shard !== shard) {
    return put({ shards, ...dest, value, createShard, fetchShard })
  }

  /** @type {API.BatcherShardEntry<T>} */
  let entry = [dest.key, value]
  /** @type {T|undefined} */
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
    batcher = createShard({
      shards,
      entries: [entry],
      prefix: pfxskeys[pfxskeys.length - 1].prefix,
      config: shard
    })

    for (let i = pfxskeys.length - 2; i > 0; i--) {
      entry = [pfxskeys[i].key, [batcher]]
      batcher = createShard({
        shards,
        entries: [entry],
        prefix: pfxskeys[i].prefix,
        config: shard
      })
    }

    entry = [pfxskeys[0].key, [batcher]]
  }

  shard.entries = Shard.putEntry(asShardEntries(shard.entries), asShardEntry(entry))

  // TODO: adjust size automatically
  const size = Shard.encodedLength(Shard.withEntries(asShardEntries(shard.entries), shard))
  if (size > shard.maxSize) {
    const common = Shard.findCommonPrefix(
      asShardEntries(shard.entries),
      entry[0]
    )
    if (!common) throw new Error('shard limit reached')
    const { prefix } = common
    /** @type {API.BatcherShardEntry<T>[]} */
    const matches = asBatcherShardEntries(common.matches)

    const entries = matches
      .filter(m => m[0] !== prefix)
      .map(m => {
        m = [...m]
        m[0] = m[0].slice(prefix.length)
        return m
      })

    const batcher = createShard({
      shards,
      entries,
      config: shard,
      prefix: shard.prefix + prefix
    })

    /** @type {API.ShardEntryShardValue<T> | API.ShardEntryShardAndValueValue<T>} */
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
 * @template {API.BatcherShard} T
 * @param {object} arg
 * @param {ShardFetcher} arg.shards
 * @param {string} arg.key
 * @param {T} arg.shard
 * @param {(arg: { shards: ShardFetcher, link: API.ShardLink, prefix: string }) => Promise<T>} arg.fetchShard
 * @returns {Promise<{ shard: T, key: string }>}
 */
export const traverse = async ({ shards, key, shard, fetchShard }) => {
  for (const e of shard.entries) {
    const [k, v] = e
    if (key === k) break
    if (key.startsWith(k) && Array.isArray(v)) {
      if (isShardLink(v[0])) {
        v[0] = await fetchShard({ shards, link: v[0], prefix: shard.prefix + k })
      }
      return traverse({ shards, key: key.slice(k.length), shard: v[0], fetchShard })
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
    if (Array.isArray(entry[1]) && !isShardLink(entry[1][0])) {
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

  const block = await Shard.encodeBlock(Shard.withEntries(entries, shard), shard.prefix)
  additions.push(block)

  if (shard.base) removals.push(shard.base)

  return { root: block.cid, additions, removals }
}

/**
 * @param {any} value
 * @returns {value is API.ShardLink}
 */
const isShardLink = (value) => Link.isLink(value)

/** @param {API.BatcherShardEntry<any>[]} entries */
const asShardEntries = entries => /** @type {API.ShardEntry[]} */ (entries)

/** @param {API.BatcherShardEntry<any>} entry */
const asShardEntry = entry => /** @type {API.ShardEntry} */ (entry)

/**
 * @template {API.BatcherShard} T
 * @param {API.ShardEntry[]} entries
 */
const asBatcherShardEntries = entries => /** @type {API.BatcherShardEntry<T>[]} */ (entries)

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root shard block.
 * @returns {Promise<API.Batcher>}
 */
export const create = (blocks, root) => {
  const shards = new ShardFetcher(blocks)
  return Batcher.create({ shards, link: root, prefix: '' })
}
