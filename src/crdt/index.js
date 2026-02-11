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
  if (!head.length) {
    const mblocks = new MemoryBlockstore()
    blocks = new MultiBlockFetcher(mblocks, blocks)
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

  return applyOperation(blocks, head, { type: 'put', key, value }, (b, r) =>
    Pail.put(b, r, key, value)
  )
}

/**
 * Delete the value for the given key from the bucket. If the key is not found
 * no operation occurs.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @param {string} key The key of the value to delete.
 * @returns {Promise<API.Result>}
 */
export const del = async (blocks, head, key) => {
  if (!head.length) throw new Error('cannot delete from empty clock')
  return applyOperation(blocks, head, { type: 'del', key }, (b, r) =>
    Pail.del(b, r, key)
  )
}

/**
 * Resolve the current root, apply an operation, create a clock event, and
 * return the updated head.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.EventLink<API.Operation>[]} head Merkle clock head.
 * @param {object} op Operation data (without root).
 * @param {(blocks: API.BlockFetcher, root: API.ShardLink) => Promise<{ root: API.ShardLink } & API.ShardDiff>} fn
 * @returns {Promise<API.Result>}
 */
const applyOperation = async (blocks, head, op, fn) => {
  const mblocks = new MemoryBlockstore()
  blocks = new MultiBlockFetcher(mblocks, blocks)

  const resolved = await root(blocks, head)
  for (const a of resolved.additions) {
    mblocks.putSync(a.cid, a.bytes)
  }

  const result = await fn(blocks, resolved.root)
  if (result.root.toString() === resolved.root.toString()) {
    return { root: resolved.root, additions: [], removals: [], head }
  }

  /** @type {API.Operation} */
  const data = { ...op, root: result.root }
  const event = await EventBlock.create(data, head)
  mblocks.putSync(event.cid, event.bytes)
  head = await Clock.advance(blocks, head, event.cid)

  /** @type {Map<string, API.ShardBlockView>} */
  const additions = new Map()
  for (const a of [...resolved.additions, ...result.additions]) {
    additions.set(a.cid.toString(), a)
  }
  /** @type {Map<string, API.ShardBlockView>} */
  const removals = new Map()
  for (const r of [...resolved.removals, ...result.removals]) {
    removals.set(r.cid.toString(), r)
  }

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
        if (op.type === 'put') {
          await batch.put(op.key, op.value)
        } else if (op.type === 'del') {
          await batch.del(op.key)
        } else {
          throw new Error(`unsupported batch operation: ${op.type}`)
        }
      }
      result = await batch.commit()
    } else {
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
    blocks = new MultiBlockFetcher(
      new MemoryBlockstore(result.additions),
      blocks
    )
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
    blocks = new MultiBlockFetcher(
      new MemoryBlockstore(result.additions),
      blocks
    )
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
  const candidates = children.map((c) => [c])
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
  arrays = arrays.map((a) => [...a])
  for (const arr of arrays) {
    for (const item of arr) {
      let matched = true
      for (const other of arrays) {
        if (arr === other) continue
        matched = other.some((i) => String(i) === String(item))
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
  const all = await Promise.all(head.map((h) => findEvents(events, h, tail)))
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
    .map(([level, events]) => [level, removeConcurrentDeletes(events)])
    .flatMap(([, es]) =>
      es.sort((a, b) => (String(a.cid) < String(b.cid) ? -1 : 1))
    )
}

/**
 * Remove concurrent events that delete the same key or delete/put the same key. The conflict resolution rules are as follows:
 * - If two or more concurrent events delete the same key, all but one of the delete events are removed (so the key is still deleted).
 * - If one or more concurrent events delete a key while another event puts a value for the same key, the put wins (i.e. the delete events are removed).
 * @param {API.EventBlockView<API.Operation>[]} events
 * @returns {API.EventBlockView<API.Operation>[]}
 */
const removeConcurrentDeletes = (events) => {
  // Simplify batch events: within a batch, ops are ordered so only the last
  // operation per key represents the net effect. e.g. [put a, put b, del a]
  // simplifies to [put b, del a] — the earlier put a is overridden.
  events = events.map((event) => {
    const { data } = event.value
    if (data.type !== 'batch') return event
    /** @type {Map<string, number>} */
    const lastIndex = new Map()
    for (let i = 0; i < data.ops.length; i++) {
      lastIndex.set(data.ops[i].key, i)
    }
    const simplified = data.ops.filter((op, i) => lastIndex.get(op.key) === i)
    if (simplified.length === data.ops.length) return event
    // @ts-expect-error creating a modified view with simplified ops
    return {
      cid: event.cid,
      bytes: event.bytes,
      value: { ...event.value, data: { ...data, ops: simplified } }
    }
  })

  /** @type {Map<string, number>} */
  const delCounts = new Map()
  /** @type {Set<string>} */
  const putKeys = new Set()

  for (const event of events) {
    const { data } = event.value
    if (data.type === 'put') {
      putKeys.add(data.key)
    } else if (data.type === 'del') {
      delCounts.set(data.key, (delCounts.get(data.key) || 0) + 1)
    } else if (data.type === 'batch') {
      for (const op of data.ops) {
        if (op.type === 'put') {
          putKeys.add(op.key)
        } else if (op.type === 'del') {
          delCounts.set(op.key, (delCounts.get(op.key) || 0) + 1)
        }
      }
    }
  }

  // Keys whose delete operations should be deduplicated: put wins over delete,
  // and multiple concurrent deletes are collapsed to one.
  /** @type {Set<string>} */
  const putWinsKeys = new Set()
  /** @type {Set<string>} */
  const dedupeDelKeys = new Set()
  for (const [key, count] of delCounts) {
    if (putKeys.has(key)) {
      putWinsKeys.add(key)
    } else if (count > 1) {
      dedupeDelKeys.add(key)
    }
  }

  if (putWinsKeys.size === 0 && dedupeDelKeys.size === 0) return events

  // Track which deduped-delete keys have already had their first delete kept
  /** @type {Set<string>} */
  const keptDeletes = new Set()

  /** @type {API.EventBlockView<API.Operation>[]} */
  const result = []
  for (const event of events) {
    const { data } = event.value
    if (data.type === 'del') {
      if (putWinsKeys.has(data.key)) continue
      if (dedupeDelKeys.has(data.key)) {
        if (keptDeletes.has(data.key)) continue
        keptDeletes.add(data.key)
      }
      result.push(event)
    } else if (data.type === 'batch') {
      const filteredOps = data.ops.filter((op) => {
        if (op.type !== 'del') return true
        if (putWinsKeys.has(op.key)) return false
        if (dedupeDelKeys.has(op.key)) {
          if (keptDeletes.has(op.key)) return false
          keptDeletes.add(op.key)
        }
        return true
      })
      if (filteredOps.length === 0) continue
      if (filteredOps.length !== data.ops.length) {
        // @ts-expect-error creating a modified view with filtered ops
        result.push({
          cid: event.cid,
          bytes: event.bytes,
          value: { ...event.value, data: { ...data, ops: filteredOps } }
        })
      } else {
        result.push(event)
      }
    } else {
      result.push(event)
    }
  }

  return result
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
  const rest = await Promise.all(
    parents.map((p) => findEvents(events, p, end, depth + 1))
  )
  return acc.concat(...rest)
}
