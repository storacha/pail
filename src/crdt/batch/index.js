// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import * as Shard from '../../shard.js'
import { ShardFetcher, ShardBlock } from '../../shard.js'
import * as Batch from '../../batch/index.js'
import { BatchCommittedError } from '../../batch/index.js'
import * as CRDT from '../index.js'
import * as Clock from '../../clock/index.js'
import { EventBlock } from '../../clock/index.js'
import { MemoryBlockstore, MultiBlockFetcher } from '../../block.js'

export { BatchCommittedError }

/** @implements {API.CRDTBatcher} */
class Batcher {
  #committed = false

  /**
   * @param {object} init
   * @param {API.BlockFetcher} init.blocks Block storage.
   * @param {API.EventLink<API.Operation>[]} init.head Merkle clock head.
   * @param {API.BatcherShardEntry[]} init.entries The entries in this shard.
   * @param {string} init.prefix Key prefix.
   * @param {number} init.version Shard compatibility version.
   * @param {string} init.keyChars Characters allowed in keys, referring to a known character set.
   * @param {number} init.maxKeySize Max key size in bytes.
   * @param {API.ShardBlockView} init.base Original shard this batcher is based on.
   * @param {API.ShardBlockView[]} init.additions Additions to include in the committed batch.
   * @param {API.ShardBlockView[]} init.removals Removals to include in the committed batch.
   */
  constructor ({ blocks, head, entries, prefix, version, keyChars, maxKeySize, base, additions, removals }) {
    this.blocks = blocks
    this.head = head
    this.prefix = prefix
    this.entries = [...entries]
    this.base = base
    this.version = version
    this.keyChars = keyChars
    this.maxKeySize = maxKeySize
    this.additions = additions
    this.removals = removals
    /** @type {API.BatchOperation['ops']} */
    this.ops = []
  }

  /**
   * @param {string} key The key of the value to put.
   * @param {API.UnknownLink} value The value to put.
   * @returns {Promise<void>}
   */
  async put (key, value) {
    if (this.#committed) throw new BatchCommittedError()
    await Batch.put(this.blocks, this, key, value)
    this.ops.push({ type: 'put', key, value })
  }

  async commit () {
    if (this.#committed) throw new BatchCommittedError()
    this.#committed = true

    const res = await Batch.commit(this)

    /** @type {API.Operation} */
    const data = { type: 'batch', ops: this.ops, root: res.root }
    const event = await EventBlock.create(data, this.head)

    const mblocks = new MemoryBlockstore()
    const blocks = new MultiBlockFetcher(mblocks, this.blocks)
    mblocks.putSync(event.cid, event.bytes)

    const head = await Clock.advance(blocks, this.head, event.cid)

    /** @type {Map<string, API.ShardBlockView>} */
    const additions = new Map()
    /** @type {Map<string, API.ShardBlockView>} */
    const removals = new Map()

    for (const a of this.additions) {
      additions.set(a.cid.toString(), a)
    }
    for (const r of this.removals) {
      removals.set(r.cid.toString(), r)
    }

    for (const a of res.additions) {
      if (removals.has(a.cid.toString())) {
        removals.delete(a.cid.toString())
      }
      additions.set(a.cid.toString(), a)
    }
    for (const r of res.removals) {
      if (additions.has(r.cid.toString())) {
        additions.delete(r.cid.toString())
      } else {
        removals.set(r.cid.toString(), r)
      }
    }

    return {
      head,
      event,
      root: res.root,
      additions: [...additions.values()],
      removals: [...removals.values()]
    }
  }

  /**
   * @param {object} init
   * @param {API.BlockFetcher} init.blocks Block storage.
   * @param {API.EventLink<API.Operation>[]} init.head Merkle clock head.
   */
  static async create ({ blocks, head }) {
    const mblocks = new MemoryBlockstore()
    blocks = new MultiBlockFetcher(mblocks, blocks)

    if (!head.length) {
      const base = await ShardBlock.create()
      mblocks.putSync(base.cid, base.bytes)
      return new Batcher({
        blocks,
        head,
        entries: [],
        base,
        additions: [base],
        removals: [],
        ...Shard.configure(base.value)
      })
    }

    const { root, additions, removals } = await CRDT.root(blocks, head)
    for (const a of additions) {
      mblocks.putSync(a.cid, a.bytes)
    }

    const shards = new ShardFetcher(blocks)
    const base = await shards.get(root)
    return new Batcher({
      blocks,
      head,
      entries: base.value.entries,
      base,
      additions,
      removals,
      ...Shard.configure(base.value)
    })
  }
}

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @returns {Promise<API.CRDTBatcher>}
 */
export const create = (blocks, head) => Batcher.create({ blocks, head })
