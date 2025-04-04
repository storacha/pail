import { Block, encode, decode } from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as cbor from '@ipld/dag-cbor'
// eslint-disable-next-line no-unused-vars
import * as API from './api.js'

/**
 * Advance the clock by adding an event.
 *
 * @template T
 * @param {API.BlockFetcher} blocks Block storage.
 * @param {API.EventLink<T>[]} head The head of the clock.
 * @param {API.EventLink<T>} event The event to add.
 */
export const advance = async (blocks, head, event) => {
  const events = new EventFetcher(blocks)
  const headmap = new Map(head.map(cid => [cid.toString(), cid]))
  if (headmap.has(event.toString())) return head

  // does event contain the clock?
  let changed = false
  for (const cid of head) {
    if (await contains(events, event, cid)) {
      headmap.delete(cid.toString())
      headmap.set(event.toString(), event)
      changed = true
    }
  }
  if (changed) {
    return [...headmap.values()]
  }

  // does clock contain the event?
  for (const p of head) {
    if (await contains(events, p, event)) {
      return head
    }
  }

  return head.concat(event)
}

/**
 * @template T
 * @extends {Block<API.EventView<T>, typeof cbor.code, typeof sha256.code, 1>}
 * @implements {API.EventBlockView<T>}
 */
export class EventBlock extends Block {
  /**
   * @param {object} config
   * @param {API.EventLink<T>} config.cid
   * @param {Event} config.value
   * @param {Uint8Array} config.bytes
   * @param {string} config.prefix
   */
  constructor ({ cid, value, bytes, prefix }) {
    // @ts-expect-error
    super({ cid, value, bytes })
    this.prefix = prefix
  }

  /**
   * @template T
   * @param {T} data
   * @param {API.EventLink<T>[]} [parents]
   */
  static create (data, parents) {
    return encodeEventBlock({ data, parents: parents ?? [] })
  }
}

/** @template T */
export class EventFetcher {
  /** @param {API.BlockFetcher} blocks */
  constructor (blocks) {
    /** @private */
    this._blocks = blocks
  }

  /**
   * @param {API.EventLink<T>} link
   * @returns {Promise<API.EventBlockView<T>>}
   */
  async get (link) {
    const block = await this._blocks.get(link)
    if (!block) throw new Error(`missing block: ${link}`)
    return decodeEventBlock(block.bytes)
  }
}

/**
 * @template T
 * @param {API.EventView<T>} value
 * @returns {Promise<API.EventBlockView<T>>}
 */
export const encodeEventBlock = async (value) => {
  if (typeof value.data === 'undefined' || !Array.isArray(value.parents)) {
    throw new Error('invalid event block structure')
  }
  const { data } = value
  const parents = [...value.parents].sort((a, b) => compareBytes(a.bytes, b.bytes))
  const { cid, bytes } = await encode({ value: { data, parents }, codec: cbor, hasher: sha256 })
  // @ts-expect-error
  return new Block({ cid, value, bytes })
}

/**
 * @param {Uint8Array} a
 * @param {Uint8Array} b
 */
const compareBytes = (a, b) => {
  for (let i = 0; i < a.byteLength; i++) {
    if (a[i] < b[i]) return -1
    if (a[i] > b[i]) return 1
  }
  if (a.byteLength > b.byteLength) return 1
  if (a.byteLength < b.byteLength) return -1
  return 0
}

/**
 * @template T
 * @param {Uint8Array} bytes
 * @returns {Promise<API.EventBlockView<T>>}
 */
export const decodeEventBlock = async (bytes) => {
  const { cid, value } = await decode({ bytes, codec: cbor, hasher: sha256 })
  // @ts-expect-error
  return new Block({ cid, value, bytes })
}

/**
 * Returns true if event "a" contains event "b". Breadth first search.
 * @template T
 * @param {EventFetcher<T>} events
 * @param {API.EventLink<T>} a
 * @param {API.EventLink<T>} b
 */
const contains = async (events, a, b) => {
  if (a.toString() === b.toString()) return true
  const [{ value: aevent }, { value: bevent }] = await Promise.all([events.get(a), events.get(b)])
  const links = [...aevent.parents]
  const seen = new Set()
  while (links.length) {
    const link = links.shift()
    if (!link) break
    if (link.toString() === b.toString()) return true
    // if any of b's parents are this link, then b cannot exist in any of the
    // tree below, since that would create a cycle.
    if (bevent.parents.some(p => link.toString() === p.toString())) continue
    if (seen.has(link.toString())) continue
    seen.add(link.toString())
    const { value: event } = await events.get(link)
    links.push(...event.parents)
  }
  return false
}

/**
 * @template T
 * @param {API.BlockFetcher} blocks Block storage.
 * @param {API.EventLink<T>[]} head
 * @param {object} [options]
 * @param {(b: API.EventBlockView<T>) => string} [options.renderNodeLabel]
 */
export const vis = async function * (blocks, head, options = {}) {
  const renderNodeLabel = options.renderNodeLabel ?? (b => shortLink(b.cid))
  const events = new EventFetcher(blocks)
  yield 'digraph clock {'
  yield '  node [shape=point fontname="Courier"]; head;'
  const hevents = await Promise.all(head.map(link => events.get(link)))
  /** @type {import('multiformats').Link<API.EventView<any>>[]} */
  const links = []
  const nodes = new Set()
  for (const e of hevents) {
    nodes.add(e.cid.toString())
    yield `  node [shape=oval fontname="Courier"]; ${e.cid} [label="${renderNodeLabel(e)}"];`
    yield `  head -> ${e.cid};`
    for (const p of e.value.parents) {
      yield `  ${e.cid} -> ${p};`
    }
    links.push(...e.value.parents)
  }
  while (links.length) {
    const link = links.shift()
    if (!link) break
    if (nodes.has(link.toString())) continue
    nodes.add(link.toString())
    const block = await events.get(link)
    yield `  node [shape=oval]; ${link} [label="${renderNodeLabel(block)}" fontname="Courier"];`
    for (const p of block.value.parents) {
      yield `  ${link} -> ${p};`
    }
    links.push(...block.value.parents)
  }
  yield '}'
}

/** @param {import('multiformats').UnknownLink} l */
const shortLink = l => `${String(l).slice(0, 4)}..${String(l).slice(-4)}`
