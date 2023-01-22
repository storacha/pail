import * as Clock from './clock'
import { EventBlock } from './clock'
import * as Pail from './index.js'

/**
 * @typedef {{
 *   type: 'put'|'del'
 *   key: string
 *   value: import('./link').AnyLink
 *   root: import('./shard').ShardLink
 * }} EventData
 */

/**
 * Put a value (a CID) for the given key. If the key exists it's value is
 * overwritten.
 *
 * @param {import('./block').BlockFetcher} blocks Bucket block storage.
 * @param {import('./clock').EventLink<EventData>[]} head Merkle clock head.
 * @param {import('./shard').ShardLink} root CID of the root node of the bucket.
 * @param {string} key The key of the value to put.
 * @param {import('./link').AnyLink} value The value to put.
 * @param {object} [options]
 * @param {number} [options.maxShardSize] Maximum shard size in bytes.
 * @returns {Promise<{ root: import('./shard').ShardLink, head: import('./clock').EventLink<EventData>[], event: import('./clock').EventBlockView<EventData> } & import('./index').ShardDiff>}
 */
export async function put (blocks, head, root, key, value, options) {
  if (head.length < 2) { // Easy case!
    const result = await Pail.put(blocks, root, key, value, options)
    /** @type {EventData} */
    const data = { type: 'put', root: result.root, key, value }
    const event = await EventBlock.create(data, head)
    head = await Clock.advance(withBlock(blocks, event), head, event.cid)
    return { ...result, head, event }
  }
  // order the data in head
  throw new Error('not implemented')
}

/**
 * @param {import('./block').BlockFetcher} bs
 * @param {import('./block').AnyBlock} b
 * @returns {import('./block').BlockFetcher}
 */
// @ts-expect-error
const withBlock = (bs, b) => ({ get: cid => String(cid) === String(b.cid) ? b : bs.get(cid) })

/**
 * @param {import('./clock').EventFetcher} events
 * @param  {import('./clock').EventLink<EventData>[]} children
 */
async function findCommonAncestor (events, children) {
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
 * @param {import('./clock').EventFetcher} events
 * @param {import('./clock').EventLink<EventData>} root
 */
async function findAncestorCandidate (events, root) {
  const event = await events.get(root)
  if (event.value.parents.length === 1) return event.value.parents[0]
  return await findCommonAncestor(events, event.value.parents)
}

/**
 * @template {{ toString: () => string }} T
 * @param  {Array<T[]>} arrays
 */
function findCommonString (arrays) {
  arrays = arrays.map(a => [...a])
  for (const arr of arrays) {
    for (const item of arr) {
      for (const other of arrays) {
        if (arr === other) continue
        const match = other.find(i => String(i) === String(item))
        if (match) return match
      }
    }
  }
}
