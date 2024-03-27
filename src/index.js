// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { ShardFetcher, isPrintableASCII } from './shard.js'
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

  if (rshard.value.keyChars !== Shard.KeyCharsASCII) {
    throw new Error(`unsupported key character set: ${rshard.value.keyChars}`)
  }
  if (!isPrintableASCII(key)) {
    throw new Error('key contains non-ASCII characters')
  }
  // ensure utf8 encoded key is smaller than max
  if (new TextEncoder().encode(key).length > rshard.value.maxKeySize) {
    throw new Error(`UTF-8 encoded key exceeds max size of ${rshard.value.maxKeySize} bytes`)
  }

  const path = await traverse(shards, rshard, key)
  const target = path[path.length - 1]
  const skey = key.slice(target.value.prefix.length) // key within the shard

  /** @type {API.ShardEntry} */
  let entry = [skey, value]
  let targetEntries = [...target.value.entries]

  /** @type {API.ShardBlockView[]} */
  const additions = []

  for (const [i, e] of targetEntries.entries()) {
    const [k, v] = e

    // is this just a replace?
    if (k === skey) break

    // do we need to shard this entry?
    const shortest = k.length < skey.length ? k : skey
    const other = shortest === k ? skey : k
    let common = ''
    for (const char of shortest) {
      const next = common + char
      if (!other.startsWith(next)) break
      common = next
    }
    if (common.length) {
      /** @type {API.ShardEntry[]} */
      let entries = []

      // if the existing entry key or new key is equal to the common prefix,
      // then the existing value / new value needs to persist in the parent
      // shard. Otherwise they persist in this new shard.
      if (common !== skey) {
        entries = Shard.putEntry(entries, [skey.slice(common.length), value])
      }
      if (common !== k) {
        entries = Shard.putEntry(entries, [k.slice(common.length), v])
      }

      let child = await Shard.encodeBlock(
        Shard.withEntries(entries, { ...target.value, prefix: target.value.prefix + common })
      )
      additions.push(child)
  
      // need to spread as access by index does not consider utf-16 surrogates
      const commonChars = [...common]

      // create parent shards for each character of the common prefix
      for (let i = commonChars.length - 1; i > 0; i--) {
        const parentConfig = { ...target.value, prefix: target.value.prefix + commonChars.slice(0, i).join('') }
        /** @type {API.ShardEntryLinkValue | API.ShardEntryValueValue | API.ShardEntryLinkAndValueValue} */
        let parentValue
        // if the first iteration and the existing entry key is equal to the
        // common prefix, then existing value needs to persist in this parent
        if (i === commonChars.length - 1 && common === k) {
          if (Array.isArray(v)) throw new Error('found a shard link when expecting a value')
          parentValue = [child.cid, v]
        } else if (i === commonChars.length - 1 && common === skey) {
          parentValue = [child.cid, value]
        } else {
          parentValue = [child.cid]
        }
        const parent = await Shard.encodeBlock(Shard.withEntries([[commonChars[i], parentValue]], parentConfig))
        additions.push(parent)
        child = parent
      }

      // remove the sharded entry
      targetEntries.splice(i, 1)

      // create the entry that will be added to target
      if (commonChars.length === 1 && common === k) {
        if (Array.isArray(v)) throw new Error('found a shard link when expecting a value')
        entry = [commonChars[0], [child.cid, v]]
      } else if (commonChars.length === 1 && common === skey) {
        entry = [commonChars[0], [child.cid, value]]
      } else {
        entry = [commonChars[0], [child.cid]]
      }
      break
    }
  }

  const shard = Shard.withEntries(Shard.putEntry(targetEntries, entry), target.value)
  let child = await Shard.encodeBlock(shard)

  // if no change in the target then we're done
  if (child.cid.toString() === target.cid.toString()) {
    return { root, additions: [], removals: [] }
  }

  additions.push(child)

  // path is root -> target, so work backwards, propagating the new shard CID
  for (let i = path.length - 2; i >= 0; i--) {
    const parent = path[i]
    const key = child.value.prefix.slice(parent.value.prefix.length)
    const value = Shard.withEntries(
      parent.value.entries.map((entry) => {
        const [k, v] = entry
        if (k !== key) return entry
        if (!Array.isArray(v)) throw new Error(`"${key}" is not a shard link in: ${parent.cid}`)
        return /** @type {API.ShardEntry} */(v[1] == null ? [k, [child.cid]] : [k, [child.cid, v[1]]])
      }),
      parent.value
    )

    child = await Shard.encodeBlock(value)
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
  const skey = key.slice(target.value.prefix.length) // key within the shard
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
  const skey = key.slice(target.value.prefix.length) // key within the shard

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

  let child = await Shard.encodeBlock(shard)
  additions.push(child)

  // path is root -> shard, so work backwards, propagating the new shard CID
  for (let i = path.length - 2; i >= 0; i--) {
    const parent = path[i]
    const key = child.value.prefix.slice(parent.value.prefix.length)
    const value = Shard.withEntries(
      parent.value.entries.map((entry) => {
        const [k, v] = entry
        if (k !== key) return entry
        if (!Array.isArray(v)) throw new Error(`"${key}" is not a shard link in: ${parent.cid}`)
        return /** @type {API.ShardEntry} */(v[1] == null ? [k, [child.cid]] : [k, [child.cid, v[1]]])
      }),
      parent.value
    )

    child = await Shard.encodeBlock(value)
    additions.push(child)
  }

  return { root: additions[additions.length - 1].cid, additions, removals }
}

/**
 * @param {API.EntriesOptions} [options]
 * @returns {options is API.KeyPrefixOption}
 */
const isKeyPrefixOption = options => {
  const opts = options ?? {}
  return 'prefix' in opts && Boolean(opts.prefix)
}

/**
 * @param {API.EntriesOptions} [options]
 * @returns {options is API.KeyRangeOption}
 */
const isKeyRangeOption = options => {
  const opts = options ?? {}
  return ('gt' in opts && Boolean(opts.gt)) || ('gte' in opts && Boolean(opts.gte)) || ('lt' in opts && Boolean(opts.lt)) || ('lte' in opts && Boolean(opts.lte))
}

/**
 * @param {API.KeyRangeOption} options
 * @returns {options is API.KeyLowerBoundRangeOption}
 */
const isKeyLowerBoundRangeOption = options => ('gt' in options && Boolean(options.gt)) || ('gte' in options && Boolean(options.gte))

/**
 * @param {API.KeyLowerBoundRangeOption} options
 * @returns {options is API.KeyLowerBoundRangeInclusiveOption}
 */
const isKeyLowerBoundRangeInclusiveOption = options => 'gte' in options && Boolean(options.gte)

/**
 * @param {API.KeyLowerBoundRangeOption} options
 * @returns {options is API.KeyLowerBoundRangeExclusiveOption}
 */
const isKeyLowerBoundRangeExclusiveOption = options => 'gt' in options && Boolean(options.gt)

/**
 * @param {API.KeyRangeOption} options
 * @returns {options is API.KeyUpperBoundRangeOption}
 */
const isKeyUpperBoundRangeOption = options => ('lt' in options && Boolean(options.lt)) || ('lte' in options && Boolean(options.lte))

/**
 * @param {API.KeyUpperBoundRangeOption} options
 * @returns {options is API.KeyUpperBoundRangeInclusiveOption}
 */
const isKeyUpperBoundRangeInclusiveOption = options => 'lte' in options && Boolean(options.lte)

/**
 * @param {API.KeyUpperBoundRangeOption} options
 * @returns {options is API.KeyUpperBoundRangeExclusiveOption}
 */
const isKeyUpperBoundRangeExclusiveOption = options => 'lt' in options && Boolean(options.lt)

/**
 * List entries in the bucket.
 *
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root node of the bucket.
 * @param {API.EntriesOptions} [options]
 * @returns {AsyncIterableIterator<API.ShardValueEntry>}
 */
export const entries = async function * (blocks, root, options) {
  const hasKeyPrefix = isKeyPrefixOption(options)
  const hasKeyRange = isKeyRangeOption(options)
  const hasKeyLowerBoundRange = hasKeyRange && isKeyLowerBoundRangeOption(options)
  const hasKeyLowerBoundRangeInclusive = hasKeyLowerBoundRange && isKeyLowerBoundRangeInclusiveOption(options)
  const hasKeyLowerBoundRangeExclusive = hasKeyLowerBoundRange && isKeyLowerBoundRangeExclusiveOption(options)
  const hasKeyUpperBoundRange = hasKeyRange && isKeyUpperBoundRangeOption(options)
  const hasKeyUpperBoundRangeInclusive = hasKeyUpperBoundRange && isKeyUpperBoundRangeInclusiveOption(options)
  const hasKeyUpperBoundRangeExclusive = hasKeyUpperBoundRange && isKeyUpperBoundRangeExclusiveOption(options)
  const hasKeyUpperAndLowerBoundRange = hasKeyLowerBoundRange && hasKeyUpperBoundRange

  const shards = new ShardFetcher(blocks)
  const rshard = await shards.get(root)

  yield * (
    /** @returns {AsyncIterableIterator<API.ShardValueEntry>} */
    async function * ents (shard) {
      for (const entry of shard.value.entries) {
        const key = shard.value.prefix + entry[0]

        // if array, this is a link to a shard
        if (Array.isArray(entry[1])) {
          if (entry[1][1]) {
            if (
              (hasKeyPrefix && key.startsWith(options.prefix)) ||
              (hasKeyUpperAndLowerBoundRange && (
                ((hasKeyLowerBoundRangeExclusive && key > options.gt) || (hasKeyLowerBoundRangeInclusive && key >= options.gte)) &&
                ((hasKeyUpperBoundRangeExclusive && key < options.lt) || (hasKeyUpperBoundRangeInclusive && key <= options.lte))
              )) ||
              (hasKeyLowerBoundRangeExclusive && key > options.gt) ||
              (hasKeyLowerBoundRangeInclusive && key >= options.gte) ||
              (hasKeyUpperBoundRangeExclusive && key < options.lt) ||
              (hasKeyUpperBoundRangeInclusive && key <= options.lte) ||
              (!hasKeyPrefix && !hasKeyRange)
            ) {
              yield [key, entry[1][1]]
            }
          }

          if (hasKeyPrefix) {
            if (options.prefix.length <= key.length && !key.startsWith(options.prefix)) {
              continue
            }
            if (options.prefix.length > key.length && !options.prefix.startsWith(key)) {
              continue
            }
          } else if (
            (hasKeyLowerBoundRangeExclusive && (key.slice(0, options.gt.length) <= options.gt)) ||
            (hasKeyLowerBoundRangeInclusive && (key.slice(0, options.gte.length) < options.gte)) ||
            (hasKeyUpperBoundRangeExclusive && (key.slice(0, options.lt.length) >= options.lt)) ||
            (hasKeyUpperBoundRangeInclusive && (key.slice(0, options.lte.length) > options.lte))
          ) {
            continue
          }
          yield * ents(await shards.get(entry[1][0]))
        } else {
          if (
            (hasKeyPrefix && key.startsWith(options.prefix)) ||
            (hasKeyRange && hasKeyUpperAndLowerBoundRange && (
              ((hasKeyLowerBoundRangeExclusive && key > options.gt) || (hasKeyLowerBoundRangeInclusive && key >= options.gte)) &&
              ((hasKeyUpperBoundRangeExclusive && key < options.lt) || (hasKeyUpperBoundRangeInclusive && key <= options.lte))
            )) ||
            (hasKeyRange && !hasKeyUpperAndLowerBoundRange && (
              (hasKeyLowerBoundRangeExclusive && key > options.gt) || (hasKeyLowerBoundRangeInclusive && key >= options.gte) ||
              (hasKeyUpperBoundRangeExclusive && key < options.lt) || (hasKeyUpperBoundRangeInclusive && key <= options.lte)
            )) ||
            (!hasKeyPrefix && !hasKeyRange)
          ) {
            yield [key, entry[1]]
          }
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
      const path = await traverse(shards, await shards.get(v[0]), key.slice(k.length))
      return [shard, ...path]
    }
  }
  return [shard]
}
