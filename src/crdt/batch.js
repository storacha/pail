// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher } from '../shard.js'
import * as Batch from '../batch/index.js'
import * as CRDT from './index.js'
import * as Clock from '../clock/index.js'
import { EventBlock } from '../clock/index.js'

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
    return Batch.put({
      shards: this.shards,
      shard: this,
      key,
      value,
      // TODO: I think we don't need these
      createShard: arg => new Batcher(arg),
      fetchShard: Batcher.create
    })
  }

  async commit () {
    const { root, additions, removals } = await Batch.commit(this)

    /** @type {API.Operation} */
    const data = { type: 'batch', ops: this.ops, root }
    const event = await EventBlock.create(data, this.head)
    const head = await Clock.advance(this.blocks, this.head, event.cid)

    return { head, event, root, additions, removals }
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
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @returns {Promise<API.Batcher>}
 */
export const create = async (blocks, head) => {
  const { root } = await CRDT.root(blocks, head)
  const shards = new ShardFetcher(blocks)
  return Batcher.create({ shards, link: root, prefix: '' })
}
