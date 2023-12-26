import * as Link from 'multiformats/link'
// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher } from './shard.js'
import * as Shard from './shard.js'

/**
 * @typedef {[
 *   key: string,
 *   value: API.ShardEntryValueValue | API.ShardEntryLinkValue | API.ShardEntryLinkAndValueValue,
 *   batcher?: Batcher
 * ]} BatcherShardEntry
 */

const placeholder = /** @type {API.ShardLink} */ (Link.parse('bafkqaaa'))

/** @implements {API.Batcher} */
class Batcher {
  /**
   * @param {object} arg
   * @param {ShardFetcher} arg.shards Shard storage.
   * @param {BatcherShardEntry[]} arg.entries The entries in this shard.
   * @param {string} arg.prefix Key prefix.
   * @param {API.ShardConfig} arg.config Shard config.
   * @param {API.ShardBlockView} [arg.base] Original shard this builder is based on.
   */
  constructor ({ shards, entries, prefix, config, base }) {
    this.shards = shards
    this.prefix = prefix
    this.entries = entries
    this.config = config
    this.base = base
  }

  /**
   * @param {string} key The key of the value to put.
   * @param {API.UnknownLink} value The value to put.
   * @returns {Promise<void>}
   */
  async put (key, value) {
    const dest = await this.traverse(key)
    if (dest.builder !== this) {
      return dest.builder.put(dest.key, value)
    }

    /** @type {BatcherShardEntry} */
    let entry = [dest.key, value]
    /** @type {Batcher|undefined} */
    let builder

    // if the key in this shard is longer than allowed, then we need to make some
    // intermediate shards.
    if (key.length > this.config.maxKeyLength) {
      const pfxskeys = Array.from(Array(Math.ceil(key.length / this.config.maxKeyLength)), (_, i) => {
        const start = i * this.config.maxKeyLength
        return {
          prefix: this.prefix + key.slice(0, start),
          key: key.slice(start, start + this.config.maxKeyLength)
        }
      })

      entry = [pfxskeys[pfxskeys.length - 1].key, value]
      builder = new Batcher({
        shards: this.shards,
        entries: [entry],
        prefix: pfxskeys[pfxskeys.length - 1].prefix,
        config: this.config
      })

      for (let i = pfxskeys.length - 2; i > 0; i--) {
        entry = [pfxskeys[i].key, [placeholder], builder]
        builder = new Batcher({
          shards: this.shards,
          entries: [entry],
          prefix: pfxskeys[pfxskeys.length - 1].prefix,
          config: this.config
        })
      }

      entry = [pfxskeys[0].key, [placeholder], builder]
    }

    this.entries = Shard.putEntry(asShardEntries(this.entries), asShardEntry(entry))

    // TODO: adjust size automatically
    const size = Shard.encodedLength(Shard.withEntries(asShardEntries(this.entries), this.config))
    if (size > this.config.maxSize) {
      const common = Shard.findCommonPrefix(
        asShardEntries(this.entries),
        entry[0]
      )
      if (!common) throw new Error('shard limit reached')
      const { prefix } = common
      const matches = asShardBuilderEntries(common.matches)

      /** @type {BatcherShardEntry[]} */
      const entries = matches
        .filter(m => m[0] !== prefix)
        .map(m => {
          m = [...m]
          m[0] = m[0].slice(prefix.length)
          return m
        })

      const builder = new Batcher({
        shards: this.shards,
        entries,
        config: this.config,
        prefix: this.prefix + prefix
      })

      /** @type {API.ShardEntryLinkValue | API.ShardEntryLinkAndValueValue} */
      let value
      const pfxmatch = matches.find(m => m[0] === prefix)
      if (pfxmatch) {
        if (Array.isArray(pfxmatch[1])) {
          // should not happen! all entries with this prefix should have been
          // placed within this shard already.
          throw new Error(`expected "${prefix}" to be a shard value but found a shard link`)
        }
        value = [placeholder, pfxmatch[1]]
      } else {
        value = [placeholder]
      }

      this.entries = Shard.putEntry(
        asShardEntries(this.entries.filter(e => matches.every(m => e[0] !== m[0]))),
        asShardEntry([prefix, value, builder])
      )
    }
  }

  /**
   * Traverse from this builder through the shard to the correct builder for
   * the passed key.
   *
   * @param {string} key
   * @returns {Promise<{ builder: Batcher, key: string }>}
   */
  async traverse (key) {
    for (const e of this.entries) {
      const [k, v] = e
      if (key === k) break
      if (key.startsWith(k) && Array.isArray(v)) {
        if (v[0] !== placeholder) {
          e[2] = await Batcher.create(this.shards, v[0], this.prefix + k)
        }
        if (!e[2]) throw new Error('missing builder')
        return e[2].traverse(key.slice(k.length))
      }
    }
    return { builder: this, key }
  }

  async commit () {
    /** @type {API.ShardBlockView[]} */
    const additions = []
    /** @type {API.ShardBlockView[]} */
    const removals = []

    /** @type {API.ShardEntry[]} */
    const entries = []
    for (const entry of this.entries) {
      if (entry[2]) {
        const result = await entry[2].commit()
        entries.push([
          entry[0],
          Array.isArray(entry[1])
            ? entry[1][1] == null
              ? [result.root]
              : [result.root, entry[1][1]]
            : entry[1]
        ])
        additions.push(...result.additions)
        removals.push(...result.removals)
      } else {
        entries.push(asShardEntry(entry))
      }
    }

    const block = await Shard.encodeBlock(Shard.withEntries(entries, this.config))
    additions.push(block)

    if (this.base) removals.push(this.base)

    return { root: block.cid, additions, removals }
  }

  /**
   * @param {ShardFetcher} shards Shard storage.
   * @param {API.ShardLink} link CID of the shard block.
   * @param {string} prefix
   */
  static async create (shards, link, prefix) {
    const base = await shards.get(link)
    return new Batcher({
      shards,
      entries: asShardBuilderEntries(base.value.entries),
      prefix,
      config: Shard.configure(base.value),
      base
    })
  }
}

/** @param {BatcherShardEntry[]} entries */
const asShardEntries = entries => /** @type {API.ShardEntry[]} */ (entries)

/** @param {BatcherShardEntry} entry */
const asShardEntry = entry => /** @type {API.ShardEntry} */ (entry)

/** @param {API.ShardEntry[]} entries */
const asShardBuilderEntries = entries => /** @type {BatcherShardEntry[]} */ (entries)

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} link CID of the shard block.
 * @returns {Promise<API.Batcher>}
 */
export const create = (blocks, link) => {
  const shards = new ShardFetcher(blocks)
  return Batcher.create(shards, link, '')
}
