// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher } from './shard.js'
import * as Shard from './shard.js'

/**
 * Put a value (a CID) for the given key. If the key exists it's value is
 * overwritten.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root node of the bucket.
 * @param {string} key The key of the value to put.
 * @param {API.UnknownLink} value The value to put.
 * @returns {Promise<{ root: API.ShardLink } & API.ShardDiff>}
 */
export const put = async (blocks, root, key, value) => {
  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)
  const path = await traverse(shards, rshard, key)
  const target = path[path.length - 1]
  const skey = key.slice(target.prefix.length) // key within the shard

  /** @type {API.ShardEntry} */
  let entry = [skey, value]

  /** @type {API.ShardBlockView[]} */
  const additions = []

  // if the key in this shard is longer than allowed, then we need to make some
  // intermediate shards.
  if (skey.length > target.value.maxKeyLength) {
    const pfxskeys = Array.from(Array(Math.ceil(skey.length / target.value.maxKeyLength)), (_, i) => {
      const start = i * target.value.maxKeyLength
      return {
        prefix: target.prefix + skey.slice(0, start),
        skey: skey.slice(start, start + target.value.maxKeyLength)
      }
    })

    let child = await Shard.encodeBlock(
      Shard.withEntries([[pfxskeys[pfxskeys.length - 1].skey, value]], target.value),
      pfxskeys[pfxskeys.length - 1].prefix
    )
    additions.push(child)

    for (let i = pfxskeys.length - 2; i > 0; i--) {
      child = await Shard.encodeBlock(
        Shard.withEntries([[pfxskeys[i].skey, [child.cid]]], target.value),
        pfxskeys[i].prefix
      )
      additions.push(child)
    }

    entry = [pfxskeys[0].skey, [child.cid]]
  }

  let shard = Shard.withEntries(Shard.putEntry(target.value.entries, entry), target.value)
  let child = await Shard.encodeBlock(shard, target.prefix)

  if (child.bytes.length > shard.maxSize) {
    const common = Shard.findCommonPrefix(shard.entries, entry[0])
    if (!common) throw new Error('shard limit reached')
    const { prefix, matches } = common
    const block = await Shard.encodeBlock(
      Shard.withEntries(
        matches
          .filter(([k]) => k !== prefix)
          .map(([k, v]) => [k.slice(prefix.length), v]),
        shard
      ),
      target.prefix + prefix
    )
    additions.push(block)

    /** @type {API.ShardEntryLinkValue | API.ShardEntryLinkAndValueValue} */
    let value
    const pfxmatch = matches.find(([k]) => k === prefix)
    if (pfxmatch) {
      if (Array.isArray(pfxmatch[1])) {
        // should not happen! all entries with this prefix should have been
        // placed within this shard already.
        throw new Error(`expected "${prefix}" to be a shard value but found a shard link`)
      }
      value = [block.cid, pfxmatch[1]]
    } else {
      value = [block.cid]
    }

    shard.entries = shard.entries.filter(e => matches.every(m => e[0] !== m[0]))
    shard = Shard.withEntries(Shard.putEntry(shard.entries, [prefix, value]), shard)
    child = await Shard.encodeBlock(shard, target.prefix)
  }

  // if no change in the target then we're done
  if (child.cid.toString() === target.cid.toString()) {
    return { root, additions: [], removals: [] }
  }

  additions.push(child)

  // path is root -> shard, so work backwards, propagating the new shard CID
  for (let i = path.length - 2; i >= 0; i--) {
    const parent = path[i]
    const key = child.prefix.slice(parent.prefix.length)
    const value = Shard.withEntries(
      parent.value.entries.map((entry) => {
        const [k, v] = entry
        if (k !== key) return entry
        if (!Array.isArray(v)) throw new Error(`"${key}" is not a shard link in: ${parent.cid}`)
        return /** @type {API.ShardEntry} */(v[1] == null ? [k, [child.cid]] : [k, [child.cid, v[1]]])
      }),
      parent.value
    )

    child = await Shard.encodeBlock(value, parent.prefix)
    additions.push(child)
  }

  return { root: additions[additions.length - 1].cid, additions, removals: path }
}

/**
 * Get the stored value for the given key from the bucket. If the key is not
 * found, `undefined` is returned.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root node of the bucket.
 * @param {string} key The key of the value to get.
 * @returns {Promise<API.UnknownLink | undefined>}
 */
export const get = async (blocks, root, key) => {
  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)
  const path = await traverse(shards, rshard, key)
  const target = path[path.length - 1]
  const skey = key.slice(target.prefix.length) // key within the shard
  const entry = target.value.entries.find(([k]) => k === skey)
  if (!entry) return
  return Array.isArray(entry[1]) ? entry[1][1] : entry[1]
}

/**
 * Delete the value for the given key from the bucket. If the key is not found
 * no operation occurs.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root node of the bucket.
 * @param {string} key The key of the value to delete.
 * @returns {Promise<{ root: API.ShardLink } & API.ShardDiff>}
 */
export const del = async (blocks, root, key) => {
  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)
  const path = await traverse(shards, rshard, key)
  const target = path[path.length - 1]
  const skey = key.slice(target.prefix.length) // key within the shard

  const entryidx = target.value.entries.findIndex(([k]) => k === skey)
  if (entryidx === -1) return { root, additions: [], removals: [] }

  const entry = target.value.entries[entryidx]
  // cannot delete a shard (without data)
  if (Array.isArray(entry[1]) && entry[1][1] == null) {
    return { root, additions: [], removals: [] }
  }

  /** @type {API.ShardBlockView[]} */
  const additions = []
  /** @type {API.ShardBlockView[]} */
  const removals = [...path]

  let shard = Shard.withEntries([...target.value.entries], target.value)

  if (Array.isArray(entry[1])) {
    // remove the value from this link+value
    shard.entries[entryidx] = [entry[0], [entry[1][0]]]
  } else {
    shard.entries.splice(entryidx, 1)
    // if now empty, remove from parent
    while (!shard.entries.length) {
      const child = path[path.length - 1]
      const parent = path[path.length - 2]
      if (!parent) break
      path.pop()
      shard = Shard.withEntries(
        parent.value.entries.filter(e => {
          if (!Array.isArray(e[1])) return true
          return e[1][0].toString() !== child.cid.toString()
        }),
        parent.value
      )
    }
  }

  let child = await Shard.encodeBlock(shard, path[path.length - 1].prefix)
  additions.push(child)

  // path is root -> shard, so work backwards, propagating the new shard CID
  for (let i = path.length - 2; i >= 0; i--) {
    const parent = path[i]
    const key = child.prefix.slice(parent.prefix.length)
    const value = Shard.withEntries(
      parent.value.entries.map((entry) => {
        const [k, v] = entry
        if (k !== key) return entry
        if (!Array.isArray(v)) throw new Error(`"${key}" is not a shard link in: ${parent.cid}`)
        return /** @type {API.ShardEntry} */(v[1] == null ? [k, [child.cid]] : [k, [child.cid, v[1]]])
      }),
      parent.value
    )

    child = await Shard.encodeBlock(value, parent.prefix)
    additions.push(child)
  }

  return { root: additions[additions.length - 1].cid, additions, removals }
}

/**
 * List entries in the bucket.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root node of the bucket.
 * @param {object} [options]
 * @param {string} [options.prefix]
 * @returns {AsyncIterableIterator<API.ShardValueEntry>}
 */
export const entries = async function * (blocks, root, options = {}) {
  const { prefix } = options
  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)

  yield * (
    /** @returns {AsyncIterableIterator<API.ShardValueEntry>} */
    async function * ents (shard) {
      for (const entry of shard.value.entries) {
        const key = shard.prefix + entry[0]

        if (Array.isArray(entry[1])) {
          if (entry[1][1]) {
            if (!prefix || (prefix && key.startsWith(prefix))) {
              yield [key, entry[1][1]]
            }
          }

          if (prefix) {
            if (prefix.length <= key.length && !key.startsWith(prefix)) {
              continue
            }
            if (prefix.length > key.length && !prefix.startsWith(key)) {
              continue
            }
          }
          yield * ents(await shards.get(entry[1][0], key))
        } else {
          if (prefix && !key.startsWith(prefix)) {
            continue
          }
          yield [key, entry[1]]
        }
      }
    }
  )(rshard)
}

/**
 * Traverse from the passed shard block to the target shard block using the
 * passed key. All traversed shards are returned, starting with the passed
 * shard and ending with the target.
 *
 * @param {ShardFetcher} shards
 * @param {API.ShardBlockView} shard
 * @param {string} key
 * @returns {Promise<[API.ShardBlockView, ...Array<API.ShardBlockView>]>}
 */
const traverse = async (shards, shard, key) => {
  for (const [k, v] of shard.value.entries) {
    if (key === k) return [shard]
    if (key.startsWith(k) && Array.isArray(v)) {
      const path = await traverse(shards, await shards.get(v[0], shard.prefix + k), key.slice(k.length))
      return [shard, ...path]
    }
  }
  return [shard]
}
