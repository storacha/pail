import { Block, encode } from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as cbor from '@ipld/dag-cbor'

/**
 * @template {import('multiformats').Link} T
 * @typedef {EventBlockView<T>[]} Head
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
  /** @type {import('./block').BlockFetcher} */
  #blocks

  /** @type {Head<T>} */
  #head

  /**
   * Instantiate a new Merkle Clock with the passed head.
   * @param {import('./block').BlockFetcher} blocks Block storage.
   * @param {Head<T>} head
   */
  constructor (blocks, head) {
    if (!head) throw new Error('missing head information')
    this.#blocks = blocks
    this.#head = head
  }

  get head () {
    return this.#head
  }

  /**
   * Advance the clock by adding a new event.
   * @param {EventView<T>} event
   */
  async advance (event) {
    if (!event.parents.length) throw new Error('missing event parent(s)')

    const block = await encodeEventBlock(event)
    const parentidx = this.#head.findIndex(p => p.cid.toString() === event.parent.toString())
    if (parentidx > -1) {
      this.#head[parentidx] = block
      return block
    }

    // Find this event in the tree



    // Find the parent.
    // If we can find the parent,
    this.#head.push(block)
    return block
  }

  /**
   * Create a brand new Merkle Clock with no event history.
   * @param {import('./block').BlockFetcher} blocks Block storage.
   * @param {T} data
   */
  static async create (blocks, data) {
    const block = await encodeEventBlock(new Event(data))
    return new Clock(blocks, [block])
  }
}

/**
 * @template {import('multiformats').Link} T
 * @implements {EventView}
 */
export class Event {
  /** @type {T} */
  #data

  /** @type {EventView<T>[]} */
  #parents

  /**
   * @param {T} data
   * @param {EventView<T>[]} [parent]
   */
  constructor (data, parents) {
    this.#data = data
    this.#parents = parents
  }

  get data () {
    return this.#data
  }

  get parents () {
    return this.#parents
  }
}

/**
 * @template {import('multiformats').Link} T
 * @param {EventView<T>} value
 * @returns {Promise<EventBlockView>}
 */
export async function encodeEventBlock (value) {
  // TODO: sort parents
  const { cid, bytes } = await encode({ value, codec: cbor, hasher: sha256 })
  return new Block({ cid, value, bytes })
}
