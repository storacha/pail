import fs from 'node:fs'
import { Readable } from 'node:stream'
import { CarWriter } from '@ipld/car'
import { CID } from 'multiformats/cid'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { put } from '../../src/index.js'
import { ShardBlock } from '../../src/shard.js'
import { MemoryBlockstore } from '../../src/block.js'

/** @param {string} str */
async function stringToCID (str) {
  const hash = await sha256.digest(new TextEncoder().encode(str))
  return CID.create(1, raw.code, hash)
}

async function main () {
  const data = await fs.promises.readFile('/usr/share/dict/words', 'utf8')
  const words = data.split(/\n/)
  const cids = await Promise.all(words.map(stringToCID))
  const blocks = new MemoryBlockstore()
  const rootblk = await ShardBlock.create()
  blocks.putSync(rootblk.cid, rootblk.bytes)

  console.time(`put x${words.length}`)
  /** @type {import('../../src/api.ts').ShardLink} */
  let root = rootblk.cid
  for (const [i, word] of words.entries()) {
    const res = await put(blocks, root, word, cids[i])
    root = res.root
    for (const b of res.additions) {
      blocks.putSync(b.cid, b.bytes)
    }
    for (const b of res.removals) {
      blocks.deleteSync(b.cid)
    }
    if (i % 1000 === 0) {
      console.log(`${Math.floor(i / words.length * 100)}%`)
    }
  }
  console.timeEnd(`put x${words.length}`)

  // @ts-expect-error
  const { writer, out } = CarWriter.create(root)
  const finishPromise = new Promise(resolve => {
    Readable.from(out).pipe(fs.createWriteStream('./pail.car')).on('finish', () => resolve(true))
  })

  for (const b of blocks.entries()) {
    // @ts-expect-error
    await writer.put(b)
  }
  await writer.close()
  await finishPromise
}

main()
