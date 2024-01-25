// eslint-disable-next-line no-unused-vars
import * as Link from 'multiformats/link'
import { tokensToLength } from 'cborg/length'
import { Token, Type } from 'cborg'
import * as API from './api.js'
import { configure } from '../shard.js'

/** Byte length of a v1, dag-cbor, sha-256 CID */
const ShardLinkByteLength = 36

const CID_TAG = new Token(Type.tag, 42)

/**
 * @param {API.BatcherShardInit} [init]
 * @returns {API.BatcherShard}
 */
export const create = init => ({
  base: init?.base,
  prefix: init?.prefix ?? '',
  entries: [...init?.entries ?? []],
  ...configure(init)
})

/** @param {API.BatcherShard} shard */
export const encodedLength = (shard) => {
  let entriesLength = 0
  for (const entry of shard.entries) {
    entriesLength += entryEncodedLength(entry)
  }
  const tokens = [
    new Token(Type.map, 3),
    new Token(Type.string, 'entries'),
    new Token(Type.array, shard.entries.length),
    new Token(Type.string, 'maxKeyLength'),
    new Token(Type.uint, shard.maxKeyLength),
    new Token(Type.string, 'maxSize'),
    new Token(Type.uint, shard.maxSize)
  ]
  return tokensToLength(tokens) + entriesLength
}

/** @param {API.BatcherShardEntry} entry */
const entryEncodedLength = entry => {
  const tokens = [
    new Token(Type.array, entry.length),
    new Token(Type.string, entry[0])
  ]
  if (Array.isArray(entry[1])) {
    tokens.push(new Token(Type.array, entry[1].length))
    for (const item of entry[1]) {
      tokens.push(CID_TAG)
      if (Link.isLink(item)) {
        tokens.push(new Token(Type.bytes, { length: item.byteLength + 1 }))
      } else {
        // `item is BatcherShard and does not have a CID yet, however, when it
        // does, it will be this long.
        tokens.push(new Token(Type.bytes, { length: ShardLinkByteLength + 1 }))
      }
    }
  } else {
    tokens.push(CID_TAG)
    tokens.push(new Token(Type.bytes, { length: entry[1].byteLength + 1 }))
  }
  return tokensToLength(tokens)
}
