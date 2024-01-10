// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { parse } from 'multiformats/link'

/** @implements {API.BlockFetcher} */
export class MemoryBlockstore {
  /** @type {Map<string, Uint8Array>} */
  #blocks = new Map()

  /**
   * @param {Array<import('multiformats').Block>} [blocks]
   */
  constructor (blocks) {
    if (blocks) {
      this.#blocks = new Map(blocks.map(b => [b.cid.toString(), b.bytes]))
    }
  }

  /** @type {API.BlockFetcher['get']} */
  async get (cid) {
    const bytes = this.#blocks.get(cid.toString())
    if (!bytes) return
    return { cid, bytes }
  }

  /**
   * @param {API.UnknownLink} cid
   * @param {Uint8Array} bytes
   */
  async put (cid, bytes) {
    this.#blocks.set(cid.toString(), bytes)
  }

  /**
   * @param {API.UnknownLink} cid
   * @param {Uint8Array} bytes
   */
  putSync (cid, bytes) {
    this.#blocks.set(cid.toString(), bytes)
  }

  /** @param {API.UnknownLink} cid */
  async delete (cid) {
    this.#blocks.delete(cid.toString())
  }

  /** @param {API.UnknownLink} cid */
  deleteSync (cid) {
    this.#blocks.delete(cid.toString())
  }

  * entries () {
    for (const [str, bytes] of this.#blocks) {
      yield { cid: parse(str), bytes }
    }
  }
}

export class MultiBlockFetcher {
  /** @type {API.BlockFetcher[]} */
  #fetchers

  /** @param {API.BlockFetcher[]} fetchers */
  constructor (...fetchers) {
    this.#fetchers = fetchers
  }

  /** @type {API.BlockFetcher['get']} */
  async get (link) {
    for (const f of this.#fetchers) {
      const v = await f.get(link)
      if (v) return v
    }
  }
}
