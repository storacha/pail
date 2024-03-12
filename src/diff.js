// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher } from './shard.js'

/**
 * @typedef {string} K
 * @typedef {[before: null, after: API.UnknownLink]} AddV
 * @typedef {[before: API.UnknownLink, after: API.UnknownLink]} UpdateV
 * @typedef {[before: API.UnknownLink, after: null]} DeleteV
 * @typedef {[key: K, value: AddV|UpdateV|DeleteV]} KV
 * @typedef {KV[]} KeysDiff
 * @typedef {{ keys: KeysDiff, shards: API.ShardDiff }} CombinedDiff
 */

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} a Base DAG.
 * @param {API.ShardLink} b Comparison DAG.
 * @returns {Promise<CombinedDiff>}
 */
export const difference = async (blocks, a, b) => {
  if (isEqual(a, b)) return { keys: [], shards: { additions: [], removals: [] } }

  const shards = new ShardFetcher(blocks)
  const [ashard, bshard] = await Promise.all([shards.get(a), shards.get(b)])

  const aents = new Map(ashard.value.entries)
  const bents = new Map(bshard.value.entries)

  const keys = /** @type {Map<K, AddV|UpdateV|DeleteV>} */(new Map())
  const additions = new Map([[bshard.cid.toString(), bshard]])
  const removals = new Map([[ashard.cid.toString(), ashard]])

  // find shards removed in B
  for (const [akey, aval] of ashard.value.entries) {
    const bval = bents.get(akey)
    if (bval) continue
    if (!Array.isArray(aval)) {
      keys.set(`${ashard.value.prefix}${akey}`, [aval, null])
      continue
    }
    // if shard link _with_ value
    if (aval[1] != null) {
      keys.set(`${ashard.value.prefix}${akey}`, [aval[1], null])
    }
    for await (const s of collect(shards, aval[0])) {
      for (const [k, v] of s.value.entries) {
        if (!Array.isArray(v)) {
          keys.set(`${s.value.prefix}${k}`, [v, null])
        } else if (v[1] != null) {
          keys.set(`${s.value.prefix}${k}`, [v[1], null])
        }
      }
      removals.set(s.cid.toString(), s)
    }
  }

  // find shards added or updated in B
  for (const [bkey, bval] of bshard.value.entries) {
    const aval = aents.get(bkey)
    if (!Array.isArray(bval)) {
      if (!aval) {
        keys.set(`${bshard.value.prefix}${bkey}`, [null, bval])
      } else if (Array.isArray(aval)) {
        keys.set(`${bshard.value.prefix}${bkey}`, [aval[1] ?? null, bval])
      } else if (!isEqual(aval, bval)) {
        keys.set(`${bshard.value.prefix}${bkey}`, [aval, bval])
      }
      continue
    }
    if (aval && Array.isArray(aval)) { // updated in B
      if (isEqual(aval[0], bval[0])) {
        if (bval[1] != null && (aval[1] == null || !isEqual(aval[1], bval[1]))) {
          keys.set(`${bshard.value.prefix}${bkey}`, [aval[1] ?? null, bval[1]])
        }
        continue // updated value?
      }
      const res = await difference(blocks, aval[0], bval[0])
      for (const shard of res.shards.additions) {
        additions.set(shard.cid.toString(), shard)
      }
      for (const shard of res.shards.removals) {
        removals.set(shard.cid.toString(), shard)
      }
      for (const [k, v] of res.keys) {
        keys.set(k, v)
      }
    } else if (aval) { // updated in B value => link+value
      if (bval[1] == null) {
        keys.set(`${bshard.value.prefix}${bkey}`, [aval, null])
      } else if (!isEqual(aval, bval[1])) {
        keys.set(`${bshard.value.prefix}${bkey}`, [aval, bval[1]])
      }
      for await (const s of collect(shards, bval[0])) {
        for (const [k, v] of s.value.entries) {
          if (!Array.isArray(v)) {
            keys.set(`${s.value.prefix}${k}`, [null, v])
          } else if (v[1] != null) {
            keys.set(`${s.value.prefix}${k}`, [null, v[1]])
          }
        }
        additions.set(s.cid.toString(), s)
      }
    } else { // added in B
      keys.set(`${bshard.value.prefix}${bkey}`, [null, bval[0]])
      for await (const s of collect(shards, bval[0])) {
        for (const [k, v] of s.value.entries) {
          if (!Array.isArray(v)) {
            keys.set(`${s.value.prefix}${k}`, [null, v])
          } else if (v[1] != null) {
            keys.set(`${s.value.prefix}${k}`, [null, v[1]])
          }
        }
        additions.set(s.cid.toString(), s)
      }
    }
  }

  // filter blocks that were added _and_ removed from B
  for (const k of removals.keys()) {
    if (additions.has(k)) {
      additions.delete(k)
      removals.delete(k)
    }
  }

  return {
    keys: [...keys.entries()].sort((a, b) => a[0] < b[0] ? -1 : 1),
    shards: { additions: [...additions.values()], removals: [...removals.values()] }
  }
}

/**
 * @param {API.UnknownLink} a
 * @param {API.UnknownLink} b
 */
const isEqual = (a, b) => a.toString() === b.toString()

/**
 * @param {import('./shard.js').ShardFetcher} shards
 * @param {API.ShardLink} root
 * @returns {AsyncIterableIterator<API.ShardBlockView>}
 */
async function * collect (shards, root) {
  const shard = await shards.get(root)
  yield shard
  for (const [, v] of shard.value.entries) {
    if (!Array.isArray(v)) continue
    yield * collect(shards, v[0])
  }
}
