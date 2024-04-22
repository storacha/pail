// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import * as Clock from '../clock/index.js'
import { EventFetcher, EventBlock } from '../clock/index.js'
import * as Pail from '../index.js'
import { ShardBlock } from '../shard.js'
import { MemoryBlockstore, MultiBlockFetcher } from '../block.js'
import * as Batch from '../batch/index.js'

/**
 * Put a value (a CID) for the given key. If the key exists it's value is
 * overwritten.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @param {string} key The key of the value to put.
 * @param {API.UnknownLink} value The value to put.
 * @returns {Promise<API.Result>}
 */
export const put = async (blocks, head, key, value) => {
  const mblocks = new MemoryBlockstore()
  blocks = new MultiBlockFetcher(mblocks, blocks)

  if (!head.length) {
    const shard = await ShardBlock.create()
    mblocks.putSync(shard.cid, shard.bytes)
    const result = await Pail.put(blocks, shard.cid, key, value)
    /** @type {API.Operation} */
    const data = { type: 'put', root: result.root, key, value }
    const event = await EventBlock.create(data, head)
    head = await Clock.advance(blocks, head, event.cid)
    return {
      root: result.root,
      additions: [shard, ...result.additions],
      removals: result.removals,
      head,
      event
    }
  }

  /** @type {EventFetcher<API.Operation>} */
  const events = new EventFetcher(blocks)
  const ancestor = await findCommonAncestor(events, head)
  if (!ancestor) throw new Error('failed to find common ancestor event')

  const aevent = await events.get(ancestor)
  let { root } = aevent.value.data

  const sorted = await findSortedEvents(events, head, ancestor)
  /** @type {Map<string, API.ShardBlockView>} */
  const additions = new Map()
  /** @type {Map<string, API.ShardBlockView>} */
  const removals = new Map()

  for (const { value: event } of sorted) {
    let result
    if (event.data.type === 'put') {
      result = await Pail.put(blocks, root, event.data.key, event.data.value)
    } else if (event.data.type === 'del') {
      result = await Pail.del(blocks, root, event.data.key)
    } else if (event.data.type === 'batch') {
      const batch = await Batch.create(blocks, root)
      for (const op of event.data.ops) {
        if (op.type !== 'put') throw new Error(`unsupported batch operation: ${op.type}`)
        await batch.put(op.key, op.value)
      }
      result = await batch.commit()
    } else {
      // @ts-expect-error type does not exist on never
      throw new Error(`unknown operation: ${event.data.type}`)
    }

    root = result.root
    for (const a of result.additions) {
      mblocks.putSync(a.cid, a.bytes)
      additions.set(a.cid.toString(), a)
    }
    for (const r of result.removals) {
      removals.set(r.cid.toString(), r)
    }
  }

  const result = await Pail.put(blocks, root, key, value)
  // if we didn't change the pail we're done
  if (result.root.toString() === root.toString()) {
    return { root, additions: [], removals: [], head }
  }

  for (const a of result.additions) {
    mblocks.putSync(a.cid, a.bytes)
    additions.set(a.cid.toString(), a)
  }
  for (const r of result.removals) {
    removals.set(r.cid.toString(), r)
  }

  /** @type {API.Operation} */
  const data = { type: 'put', root: result.root, key, value }
  const event = await EventBlock.create(data, head)
  mblocks.putSync(event.cid, event.bytes)
  head = await Clock.advance(blocks, head, event.cid)

  // filter blocks that were added _and_ removed
  for (const k of removals.keys()) {
    if (additions.has(k)) {
      additions.delete(k)
      removals.delete(k)
    }
  }

  return {
    root: result.root,
    additions: [...additions.values()],
    removals: [...removals.values()],
    head,
    event
  }
}

/**
 * Delete the value for the given key from the bucket. If the key is not found
 * no operation occurs.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @param {string} key The key of the value to delete.
 * @param {object} [options]
 * @returns {Promise<API.Result>}
 */
export const del = async (blocks, head, key, options) => {
  throw new Error('not implemented')
}

/**
 * Determine the effective pail root given the current merkle clock head.
 *
 * Clocks with multiple head events may return blocks that were added or
 * removed while playing forward events from their common ancestor.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @returns {Promise<{ root: API.ShardLink } & API.ShardDiff>}
 */
export const root = async (blocks, head) => {
  if (!head.length) throw new Error('cannot determine root of headless clock')

  const mblocks = new MemoryBlockstore()
  blocks = new MultiBlockFetcher(mblocks, blocks)

  /** @type {EventFetcher<API.Operation>} */
  const events = new EventFetcher(blocks)

  if (head.length === 1) {
    const event = await events.get(head[0])
    const { root } = event.value.data
    return { root, additions: [], removals: [] }
  }

  const ancestor = await findCommonAncestor(events, head)
  if (!ancestor) throw new Error('failed to find common ancestor event')

  const aevent = await events.get(ancestor)
  let { root } = aevent.value.data

  const sorted = await findSortedEvents(events, head, ancestor)
  /** @type {Map<string, API.ShardBlockView>} */
  const additions = new Map()
  /** @type {Map<string, API.ShardBlockView>} */
  const removals = new Map()

  for (const { value: event } of sorted) {
    let result
    if (event.data.type === 'put') {
      result = await Pail.put(blocks, root, event.data.key, event.data.value)
    } else if (event.data.type === 'del') {
      result = await Pail.del(blocks, root, event.data.key)
    } else if (event.data.type === 'batch') {
      const batch = await Batch.create(blocks, root)
      for (const op of event.data.ops) {
        if (op.type !== 'put') throw new Error(`unsupported batch operation: ${op.type}`)
        await batch.put(op.key, op.value)
      }
      result = await batch.commit()
    } else {
      // @ts-expect-error type does not exist on never
      throw new Error(`unknown operation: ${event.data.type}`)
    }

    root = result.root
    for (const a of result.additions) {
      mblocks.putSync(a.cid, a.bytes)
      additions.set(a.cid.toString(), a)
    }
    for (const r of result.removals) {
      removals.set(r.cid.toString(), r)
    }
  }

  // filter blocks that were added _and_ removed
  for (const k of removals.keys()) {
    if (additions.has(k)) {
      additions.delete(k)
      removals.delete(k)
    }
  }

  return {
    root,
    additions: [...additions.values()],
    removals: [...removals.values()]
  }
}

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @param {string} key The key of the value to retrieve.
 */
export const get = async (blocks, head, key) => {
  if (!head.length) return
  const result = await root(blocks, head)
  if (result.additions.length) {
    blocks = new MultiBlockFetcher(new MemoryBlockstore(result.additions), blocks)
  }
  return Pail.get(blocks, result.root, key)
}

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @param {API.EntriesOptions} [options]
 */
export const entries = async function * (blocks, head, options) {
  if (!head.length) return
  const result = await root(blocks, head)
  if (result.additions.length) {
    blocks = new MultiBlockFetcher(new MemoryBlockstore(result.additions), blocks)
  }
  yield * Pail.entries(blocks, result.root, options)
}

/**
 * Find the common ancestor event of the passed children. A common ancestor is
 * the first single event in the DAG that _all_ paths from children lead to.
 *
 * @param {EventFetcher<API.Operation>} events
 * @param  {API.EventLink<API.Operation>[]} children
 */
const findCommonAncestor = async (events, children) => {
  if (!children.length) return
  const candidates = children.map(c => [c])
  while (true) {
    let changed = false
    for (const c of candidates) {
      const candidate = await findAncestorCandidate(events, c[c.length - 1])
      if (!candidate) continue
      changed = true
      c.push(candidate)
      const ancestor = findCommonString(candidates)
      if (ancestor) return ancestor
    }
    if (!changed) return
  }
}

/**
 * @param {EventFetcher<API.Operation>} events
 * @param {API.EventLink<API.Operation>} root
 */
const findAncestorCandidate = async (events, root) => {
  const { value: event } = await events.get(root)
  if (!event.parents.length) return root
  return event.parents.length === 1
    ? event.parents[0]
    : findCommonAncestor(events, event.parents)
}

/**
 * @template {{ toString: () => string }} T
 * @param  {Array<T[]>} arrays
 */
const findCommonString = (arrays) => {
  arrays = arrays.map(a => [...a])
  for (const arr of arrays) {
    for (const item of arr) {
      let matched = true
      for (const other of arrays) {
        if (arr === other) continue
        matched = other.some(i => String(i) === String(item))
        if (!matched) break
      }
      if (matched) return item
    }
  }
}

/**
 * Find and sort events between the head(s) and the tail.
 * @param {EventFetcher<API.Operation>} events
 * @param {API.EventLink<API.Operation>[]} head
 * @param {API.EventLink<API.Operation>} tail
 */
const findSortedEvents = async (events, head, tail) => {
  if (head.length === 1 && head[0].toString() === tail.toString()) {
    return []
  }

  // get weighted events - heavier events happened first
  /** @type {Map<string, { event: API.EventBlockView<API.Operation>, weight: number }>} */
  const weights = new Map()
  const all = await Promise.all(head.map(h => findEvents(events, h, tail)))
  for (const arr of all) {
    for (const { event, depth } of arr) {
      const info = weights.get(event.cid.toString())
      if (info) {
        info.weight += depth
      } else {
        weights.set(event.cid.toString(), { event, weight: depth })
      }
    }
  }

  // group events into buckets by weight
  /** @type {Map<number, API.EventBlockView<API.Operation>[]>} */
  const buckets = new Map()
  for (const { event, weight } of weights.values()) {
    const bucket = buckets.get(weight)
    if (bucket) {
      bucket.push(event)
    } else {
      buckets.set(weight, [event])
    }
  }

  // sort by weight, and by CID within weight
  return Array.from(buckets)
    .sort((a, b) => b[0] - a[0])
    .flatMap(([, es]) => es.sort((a, b) => String(a.cid) < String(b.cid) ? -1 : 1))
}

/**
 * @param {EventFetcher<API.Operation>} events
 * @param {API.EventLink<API.Operation>} start
 * @param {API.EventLink<API.Operation>} end
 * @returns {Promise<Array<{ event: API.EventBlockView<API.Operation>, depth: number }>>}
 */
const findEvents = async (events, start, end, depth = 0) => {
  const event = await events.get(start)
  const acc = [{ event, depth }]
  const { parents } = event.value
  if (parents.length === 1 && String(parents[0]) === String(end)) return acc
  const rest = await Promise.all(parents.map(p => findEvents(events, p, end, depth + 1)))
  return acc.concat(...rest)
}
