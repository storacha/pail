// import { create, load } from 'prolly-trees/db-index'
import { create, load } from '../../../../prolly-trees/src/db-index.js'

import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { nocache as cache } from 'prolly-trees/cache'
import { bf, simpleCompare } from 'prolly-trees/utils'
import { makeGetBlock } from './prolly.js'
import { cidsToProof } from './fireproof.js'
import { CID } from 'multiformats'

import * as codec from '@ipld/dag-cbor'
// import { create as createBlock } from 'multiformats/block'
import { doTransaction } from './blockstore.js'
import charwise from 'charwise'

const ALWAYS_REBUILD = false // todo: make false

// const arrayCompare = (a, b) => {
//   if (Array.isArray(a) && Array.isArray(b)) {
//     const len = Math.min(a.length, b.length)
//     for (let i = 0; i < len; i++) {
//       const comp = simpleCompare(a[i], b[i])
//       if (comp !== 0) {
//         return comp
//       }
//     }
//     return simpleCompare(a.length, b.length)
//   } else {
//     return simpleCompare(a, b)
//   }
// }

const compare = (a, b) => {
  const [aKey, aRef] = a
  const [bKey, bRef] = b
  const comp = simpleCompare(aKey, bKey)
  if (comp !== 0) return comp
  return refCompare(aRef, bRef)
}

const refCompare = (aRef, bRef) => {
  if (Number.isNaN(aRef)) return -1
  if (Number.isNaN(bRef)) throw new Error('ref may not be Infinity or NaN')
  if (aRef === Infinity) return 1 // need to test this on equal docids!
  // if (!Number.isFinite(bRef)) throw new Error('ref may not be Infinity or NaN')
  return simpleCompare(aRef, bRef)
}

const dbIndexOpts = { cache, chunker: bf(3), codec, hasher, compare }
const idIndexOpts = { cache, chunker: bf(3), codec, hasher, compare: simpleCompare }

const makeDoc = ({ key, value }) => ({ _id: key, ...value })

/**
 * JDoc for the result row type.
 * @typedef {Object} ChangeEvent
 * @property {string} key - The key of the document.
 * @property {Object} value - The new value of the document.
 * @property {boolean} [del] - Is the row deleted?
 * @memberof DbIndex
 */

/**
 * JDoc for the result row type.
 * @typedef {Object} DbIndexEntry
 * @property {string[]} key - The key for the DbIndex entry.
 * @property {Object} value - The value of the document.
 * @property {boolean} [del] - Is the row deleted?
 * @memberof DbIndex
 */

/**
 * Transforms a set of changes to DbIndex entries using a map function.
 *
 * @param {ChangeEvent[]} changes
 * @param {Function} mapFun
 * @returns {DbIndexEntry[]} The DbIndex entries generated by the map function.
 * @private
 * @memberof DbIndex
 */
const indexEntriesForChanges = (changes, mapFun) => {
  const indexEntries = []
  changes.forEach(({ key, value, del }) => {
    if (del || !value) return
    mapFun(makeDoc({ key, value }), (k, v) => {
      indexEntries.push({
        key: [charwise.encode(k), key],
        value: v
      })
    })
  })
  return indexEntries
}

/**
 * Represents an DbIndex for a Fireproof database.
 *
 * @class DbIndex
 * @classdesc An DbIndex can be used to order and filter the documents in a Fireproof database.
 *
 * @param {Fireproof} database - The Fireproof database instance to DbIndex.
 * @param {Function} mapFun - The map function to apply to each entry in the database.
 *
 */
export default class DbIndex {
  constructor (database, mapFun) {
    /**
     * The database instance to DbIndex.
     * @type {Fireproof}
     */
    this.database = database
    /**
     * The map function to apply to each entry in the database.
     * @type {Function}
     */
    this.mapFun = mapFun

    this.database.indexes.set(mapFun.toString(), this)

    this.indexById = { root: null, cid: null }
    this.indexByKey = { root: null, cid: null }

    this.dbHead = null

    this.instanceId = this.database.instanceId + `.DbIndex.${Math.random().toString(36).substring(2, 7)}`

    this.updateIndexPromise = null
  }

  toJSON () {
    return { code: this.mapFun?.toString(), clock: { db: this.dbHead?.map(cid => cid.toString()), byId: this.indexById.cid?.toString(), byKey: this.indexByKey.cid?.toString() } }
  }

  static fromJSON (database, { code, clock: { byId, byKey, db } }) {
    let mapFun
    // eslint-disable-next-line
    eval("mapFun = "+ code)
    const index = new DbIndex(database, mapFun)
    index.indexById.cid = CID.parse(byId)
    index.indexByKey.cid = CID.parse(byKey)
    index.dbHead = db.map(cid => CID.parse(cid))
    return index
  }

  /**
   * JSDoc for Query type.
   * @typedef {Object} DbQuery
   * @property {string[]} [range] - The range to query.
   * @memberof DbIndex
   */

  /**
   * Query object can have {range}
   * @param {DbQuery} query - the query range to use
   * @returns {Promise<{rows: Array<{id: string, key: string, value: any}>}>}
   * @memberof DbIndex
   * @instance
   */
  async query (query) {
    // if (!root) {
    // pass a root to query a snapshot
    await this.#updateIndex(this.database.blocks)

    // }
    const response = await doIndexQuery(this.database.blocks, this.indexByKey, query)
    return {
      proof: { index: await cidsToProof(response.cids) },
      // TODO fix this naming upstream in prolly/db-DbIndex?
      rows: response.result.map(({ id, key, row }) => {
        // console.log('query', id, key, row)
        return ({ id, key: charwise.decode(key), value: row })
      })
    }
  }

  /**
   * Update the DbIndex with the latest changes
   * @private
   * @returns {Promise<void>}
   */

  async #updateIndex (blocks) {
    if (this.updateIndexPromise) return this.updateIndexPromise
    this.updateIndexPromise = this.#innerUpdateIndex(blocks)
    this.updateIndexPromise.finally(() => { this.updateIndexPromise = null })
    return this.updateIndexPromise
  }

  async #innerUpdateIndex (inBlocks) {
    // const callTag = Math.random().toString(36).substring(4)
    // console.log(`#updateIndex ${callTag} >`, this.instanceId, this.dbHead?.toString(), this.dbIndexRoot?.cid.toString(), this.indexByIdRoot?.cid.toString())
    // todo remove this hack
    if (ALWAYS_REBUILD) {
      this.dbHead = null // hack
      this.indexByKey = null // hack
      this.dbIndexRoot = null
    }
    const result = await this.database.changesSince(this.dbHead) // {key, value, del}
    if (result.rows.length === 0) {
      // console.log('#updateIndex < no changes')
      this.dbHead = result.clock
      return
    }
    await doTransaction('#updateIndex', inBlocks, async (blocks) => {
      let oldIndexEntries = []
      let removeByIdIndexEntries = []
      if (this.dbHead) { // need a maybe load
        const oldChangeEntries = await this.indexById.root.getMany(result.rows.map(({ key }) => key))
        oldIndexEntries = oldChangeEntries.result.map((key) => ({ key, del: true }))
        removeByIdIndexEntries = oldIndexEntries.map(({ key }) => ({ key: key[1], del: true }))
      }
      const indexEntries = indexEntriesForChanges(result.rows, this.mapFun)
      const byIdIndexEntries = indexEntries.map(({ key }) => ({ key: key[1], value: key }))
      this.indexById = await bulkIndex(blocks, this.indexById, removeByIdIndexEntries.concat(byIdIndexEntries), idIndexOpts)
      this.indexByKey = await bulkIndex(blocks, this.indexByKey, oldIndexEntries.concat(indexEntries), dbIndexOpts)
      this.dbHead = result.clock
    })
    // console.log(`#updateIndex ${callTag} <`, this.instanceId, this.dbHead?.toString(), this.dbIndexRoot?.cid.toString(), this.indexByIdRoot?.cid.toString())
  }
}

/**
 * Update the DbIndex with the given entries
 * @param {Blockstore} blocks
 * @param {Block} inRoot
 * @param {DbIndexEntry[]} indexEntries
 * @private
 */
async function bulkIndex (blocks, inIndex, indexEntries, opts) {
  if (!indexEntries.length) return inIndex
  const putBlock = blocks.put.bind(blocks)
  const { getBlock } = makeGetBlock(blocks)
  let returnRootBlock
  let returnNode
  if (!inIndex.root) {
    const cid = inIndex.cid
    if (!cid) {
      for await (const node of await create({ get: getBlock, list: indexEntries, ...opts })) {
        const block = await node.block
        await putBlock(block.cid, block.bytes)
        returnRootBlock = block
        returnNode = node
      }
      return { root: returnNode, cid: returnRootBlock.cid }
    }
    inIndex.root = await load({ cid, get: getBlock, ...dbIndexOpts })
  }
  const { root, blocks: newBlocks } = await inIndex.root.bulk(indexEntries)
  returnRootBlock = await root.block
  returnNode = root
  for await (const block of newBlocks) {
    await putBlock(block.cid, block.bytes)
  }
  await putBlock(returnRootBlock.cid, returnRootBlock.bytes)
  return { root: returnNode, cid: returnRootBlock.cid }
}

async function doIndexQuery (blocks, indexByKey, query) {
  if (!indexByKey.root) {
    const cid = indexByKey.cid
    if (!cid) return { result: [] }
    const { getBlock } = makeGetBlock(blocks)
    indexByKey.root = await load({ cid, get: getBlock, ...dbIndexOpts })
  }
  if (query.range) {
    const encodedRange = query.range.map((key) => charwise.encode(key))
    return indexByKey.root.range(...encodedRange)
  } else if (query.key) {
    const encodedKey = charwise.encode(query.key)
    return indexByKey.root.get(encodedKey)
  }
}
