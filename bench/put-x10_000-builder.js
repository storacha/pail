// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import * as Builder from '../src/builder.js'
import { ShardBlock } from '../src/shard.js'
import { MemoryBlockstore } from '../src/block.js'
import { randomCID, randomString, randomInteger } from '../test/helpers.js'

const NUM = 10_000

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

  console.log('bench')
  console.time(`put x${NUM}`)
  const builder = await Builder.create(blocks, rootBlock.cid)
  for (let i = 0; i < kvs.length; i++) {
    await builder.put(kvs[i][0], kvs[i][1])
    if (i % 1000 === 0) {
      process.stdout.write('.')
    }
  }
  const result = await builder.build()
  for (const b of result.additions) {
    blocks.putSync(b.cid, b.bytes)
  }
  for (const b of result.removals) {
    blocks.deleteSync(b.cid)
  }
  console.log('')
  console.timeEnd(`put x${NUM}`)
}

main()
