import crypto from 'node:crypto'
import assert from 'node:assert'
import * as Link from 'multiformats/link'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { decodeBlock } from '../src/shard.js'
import { MemoryBlockstore } from '../src/block.js'

/**
 * @param {number} min
 * @param {number} max
 */
export function randomInteger (min, max) {
  min = Math.ceil(min)
  max = Math.floor(max)
  return Math.floor(Math.random() * (max - min) + min)
}

const Alphabet = 'abcdefghijklmnopqrstuvwxyz-/_'

/**
 * @param {number} size
 */
export function randomString (size, alphabet = Alphabet) {
  let str = ''
  while (str.length < size) {
    str += alphabet[randomInteger(0, alphabet.length)]
  }
  return str
}

/** @param {number} [size] Number of random bytes to hash. */
export async function randomCID (size = 32) {
  const hash = await sha256.digest(await randomBytes(size))
  return Link.create(raw.code, hash)
}

/** @param {number} size */
export async function randomBytes (size) {
  const bytes = new Uint8Array(size)
  while (size) {
    const chunk = new Uint8Array(Math.min(size, 65_536))
    crypto.getRandomValues(chunk)
    size -= bytes.length
    bytes.set(chunk, size)
  }
  return bytes
}

export class Blockstore extends MemoryBlockstore {
  /**
   * @param {import('../src/api.js').ShardLink} cid
   * @param {string} [prefix]
   */
  async getShardBlock (cid, prefix) {
    const blk = await this.get(cid)
    assert(blk)
    return decodeBlock(blk.bytes, prefix)
  }
}
