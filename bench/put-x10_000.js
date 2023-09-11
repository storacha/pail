import { ShardBlock, put } from '../src/index.js'
import { MemoryBlockstore } from '../src/block.js'
import { randomCID, randomString, randomInteger } from '../test/helpers.js'

const NUM = 10_000

async function main () {
  console.log('setup')

  const rootBlock = await ShardBlock.create()
  const blocks = new MemoryBlockstore()
  await blocks.put(rootBlock.cid, rootBlock.bytes)

  /** @type {Array<[string, import('multiformats').UnknownLink]>} */
  const kvs = []

  for (let i = 0; i < NUM; i++) {
    const k = randomString(randomInteger(1, 64))
    const v = await randomCID(randomInteger(8, 128))
    kvs.push([k, v])
  }

  console.log('bench')
  console.time(`put x${NUM}`)
  /** @type {import('../src/shard.js').ShardLink} */
  let root = rootBlock.cid
  for (let i = 0; i < kvs.length; i++) {
    const result = await put(blocks, root, kvs[i][0], kvs[i][1])
    for (const b of result.additions) {
      blocks.putSync(b.cid, b.bytes)
    }
    for (const b of result.removals) {
      blocks.deleteSync(b.cid)
    }
    root = result.root
    if (i % 1000 === 0) {
      process.stdout.write('.')
    }
  }
  console.log('')
  console.timeEnd(`put x${NUM}`)
}

main()
