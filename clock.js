import { Block, encode, decode } from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as cbor from '@ipld/dag-cbor'

/**
 * @template {import('multiformats').Link} T
 * @typedef {EventLink<T>[]} Head
 */

/**
 * @template {import('multiformats').Link} T
 * @typedef {{ parents: EventLink<T>[], data: T }} EventView
 */

/**
 * @template {import('multiformats').Link} T
 * @typedef {import('multiformats').BlockView<EventView<T>>} EventBlockView
 */

/**
 * @template {import('multiformats').Link} T
 * @typedef {import('multiformats').Link<EventView<T>>} EventLink
 */

/** @template {import('multiformats').Link} T */
export class Clock {
  /** @type {EventFetcher} */
  #events

  /** @type {Head<T>} */
  #head

  /**
   * Instantiate a new Merkle Clock with the passed head.
   * @param {import('./block').BlockFetcher} blocks Block storage.
   * @param {Head<T>} [head]
   */
  constructor (blocks, head) {
    if (!head) throw new Error('missing head information')
    this.#events = new EventFetcher(blocks)
    this.#head = head ?? []
  }

  get head () {
    return this.#head
  }

  /**
   * Advance the clock by adding an event.
   * @param {EventView<T>} event
   */
  async advance (event) {
    if (!event.parents.length) throw new Error('missing event parent(s)')

    const block = await encodeEventBlock(event)
    if (this.#head.some(l => l.toString() === block.cid.toString())) {
      return
    }

    for (const [i, p] of this.#head.entries()) {
      if (await contains(this.#events, block.cid, p)) {
        this.#head[i] = block.cid
        // TODO: what about the other entries?
        return block
      }
      if (await contains(this.#events, p, block.cid)) {
        return
      }
    }

    this.#head.push(block.cid)
    return block
  }
}

/**
 * @template {import('multiformats').Link} T
 * @implements {EventView<T>}
 */
export class Event {
  /** @type {T} */
  #data

  /** @type {EventLink<T>[]} */
  #parents

  /**
   * @param {T} data
   * @param {EventLink<T>[]} [parents]
   */
  constructor (data, parents) {
    this.#data = data
    this.#parents = parents ?? []
  }

  get data () {
    return this.#data
  }

  get parents () {
    return this.#parents
  }
}

/** @template {import('multiformats').Link} T */
export class EventFetcher {
  /** @param {import('./block').BlockFetcher} blocks */
  constructor (blocks) {
    /** @private */
    this._blocks = blocks
  }

  /**
   * @param {EventLink<T>} link
   * @returns {Promise<EventBlockView<T>>}
   */
  async get (link) {
    const block = await this._blocks.get(link)
    if (!block) throw new Error(`missing block: ${link}`)
    return decodeEventBlock(block.bytes)
  }
}

/**
 * @template {import('multiformats').Link} T
 * @param {EventView<T>} value
 * @returns {Promise<EventBlockView<T>>}
 */
export async function encodeEventBlock (value) {
  // TODO: sort parents
  const { cid, bytes } = await encode({ value, codec: cbor, hasher: sha256 })
  // @ts-expect-error
  return new Block({ cid, value, bytes })
}

/**
 * @template {import('multiformats').Link} T
 * @param {Uint8Array} bytes
 * @returns {Promise<EventBlockView<T>>}
 */
export async function decodeEventBlock (bytes) {
  const { cid, value } = await decode({ bytes, codec: cbor, hasher: sha256 })
  if (!Array.isArray(value)) throw new Error(`invalid shard: ${cid}`)
  // @ts-expect-error
  return new Block({ cid, value, bytes })
}

/**
 * Returns true if event a contains event b. Breadth first search.
 * @template {import('multiformats').Link} T
 * @param {EventFetcher} events
 * @param {EventLink<T>} a
 * @param {EventLink<T>} b
 */
async function contains (events, a, b) {
  if (a.toString() === b.toString()) return true
  const { value: event } = await events.get(a)
  const links = [...event.parents]
  while (links.length) {
    const link = links.shift()
    if (!link) break
    if (link.toString() === b.toString()) return true
    const { value: event } = await events.get(link)
    links.push(...event.parents)
  }
  return false
}
