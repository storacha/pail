// eslint-disable-next-line no-unused-vars
import * as API from '../src/v1/api.js'
import { put } from '../src/v1/index.js'
import { ShardBlock } from '../src/v1/shard.js'
import { MemoryBlockstore } from '../src/block.js'
import { randomCID, randomString, randomInteger } from '../test/helpers.js'
import { collectMetrics, verify, writePail } from './util.js'

const NUM = 1_000_000

async function main () {
  console.log('setup')

  const rootBlock = await ShardBlock.create()
  const blocks = new MemoryBlockstore()
  await blocks.put(rootBlock.cid, rootBlock.bytes)

  /** @type {Array<[string, API.UnknownLink]>} */
  const kvs = []
  for (let i = 0; i < NUM; i++) {
    const k = randomString(randomInteger(1, 64))
    const v = await randomCID(randomInteger(8, 128))
    kvs.push([k, v])
  }

  /** @type {API.ShardLink} */
  let root = rootBlock.cid
  console.log('bench')
  console.time(`put x${NUM}`)

  try {
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
  } catch (err) {
    console.log('')
    console.error(err)
  } finally {
    console.log('')
    console.timeEnd(`put x${NUM}`)
    await writePail(blocks, root)
    console.log(await collectMetrics(blocks, root))
    await verify(blocks, root, new Map(kvs))
  }
}

main()
