#!/usr/bin/env node
import fs from 'fs'
import os from 'os'
import { join } from 'path'
import { Readable, Writable } from 'stream'
import sade from 'sade'
import { CID } from 'multiformats/cid'
import { CARReaderStream, CARWriterStream } from 'carstream'
import clc from 'cli-color'
import archy from 'archy'
// eslint-disable-next-line no-unused-vars
import * as API from './src/api.js'
import { put, get, del, entries } from './src/index.js'
import { ShardFetcher, ShardBlock, isShardLink } from './src/shard.js'
import { difference } from './src/diff.js'
import { merge } from './src/merge.js'
import { MemoryBlockstore, MultiBlockFetcher } from './src/block.js'

const cli = sade('pail')
  .option('--path', 'Path to data store.', './pail.car')

cli.command('put <key> <value>')
  .describe('Put a value (a CID) for the given key. If the key exists it\'s value is overwritten.')
  .alias('set')
  .action(async (key, value, opts) => {
    const { root: prevRoot, blocks } = await openPail(opts.path)
    const { root, additions, removals } = await put(blocks, prevRoot, key, CID.parse(value))
    await updatePail(opts.path, blocks, root, { additions, removals })

    console.log(clc.red(`--- ${prevRoot}`))
    console.log(clc.green(`+++ ${root}`))
    console.log(clc.magenta('@@ -1 +1 @@'))
    additions.forEach(b => console.log(clc.green(`+${b.cid}`)))
    removals.forEach(b => console.log(clc.red(`-${b.cid}`)))
  })

cli.command('get <key>')
  .describe('Get the stored value for the given key from the pail. If the key is not found, `undefined` is returned.')
  .action(async (key, opts) => {
    const { root, blocks } = await openPail(opts.path)
    const value = await get(blocks, root, key)
    if (value) console.log(value.toString())
  })

cli.command('del <key>')
  .describe('Delete the value for the given key from the pail. If the key is not found no operation occurs.')
  .alias('delete', 'rm', 'remove')
  .action(async (key, opts) => {
    const { root: prevRoot, blocks } = await openPail(opts.path)
    const { root, additions, removals } = await del(blocks, prevRoot, key)
    await updatePail(opts.path, blocks, root, { additions, removals })

    console.log(clc.red(`--- ${prevRoot}`))
    console.log(clc.green(`+++ ${root}`))
    console.log(clc.magenta('@@ -1 +1 @@'))
    additions.forEach(b => console.log(clc.green(`+ ${b.cid}`)))
    removals.forEach(b => console.log(clc.red(`- ${b.cid}`)))
  })

cli.command('ls')
  .describe('List entries in the pail.')
  .alias('list')
  .option('-p, --prefix', 'Key prefix to filter by.')
  .option('--gt', 'Filter results by keys greater than this string.')
  .option('--lt', 'Filter results by keys less than this string.')
  .option('--json', 'Format output as newline delimted JSON.')
  .action(async (opts) => {
    const { root, blocks } = await openPail(opts.path)
    let n = 0
    for await (const [k, v] of entries(blocks, root, { prefix: opts.prefix, gt: opts.gt, lt: opts.lt })) {
      console.log(opts.json ? JSON.stringify({ key: k, value: v.toString() }) : `${k}\t${v}`)
      n++
    }
    if (!opts.json) console.log(`total ${n}`)
  })

cli.command('tree')
  .describe('Visualise the pail.')
  .alias('vis')
  .action(async (opts) => {
    const { root, blocks } = await openPail(opts.path)
    const shards = new ShardFetcher(blocks)
    const rshard = await shards.get(root)

    /** @type {archy.Data} */
    const archyRoot = { label: `Shard(${clc.yellow(rshard.cid.toString())}) ${rshard.bytes.length + 'b'}`, nodes: [] }

    /** @param {API.ShardEntry} entry */
    const getData = async ([k, v]) => {
      if (!Array.isArray(v)) {
        return { label: `Key(${clc.magenta(k)})`, nodes: [{ label: `Value(${clc.cyan(v)})` }] }
      }
      /** @type {archy.Data} */
      const data = { label: `Key(${clc.magenta(k)})`, nodes: [] }
      if (v[1]) data.nodes?.push({ label: `Value(${clc.cyan(v[1])})` })
      const blk = await shards.get(v[0])
      data.nodes?.push({
        label: `Shard(${clc.yellow(v[0])}) ${blk.bytes.length + 'b'}`,
        nodes: await Promise.all(blk.value.entries.map(e => getData(e)))
      })
      return data
    }

    for (const entry of rshard.value.entries) {
      archyRoot.nodes?.push(await getData(entry))
    }

    console.log(archy(archyRoot))
  })

cli.command('diff <path>')
  .describe('Find the differences between this pail and the passed pail.')
  .option('-k, --keys', 'Output key/value diff.')
  .action(async (path, opts) => {
    const [
      { root: aroot, blocks: ablocks },
      { root: broot, blocks: bblocks }
    ] = await Promise.all([openPail(opts.path), openPail(path)])
    if (aroot.toString() === broot.toString()) return

    const fetcher = new MultiBlockFetcher(ablocks, bblocks)
    const { shards: { additions, removals }, keys } = await difference(fetcher, aroot, broot)

    console.log(clc.red(`--- ${aroot}`))
    console.log(clc.green(`+++ ${broot}`))
    console.log(clc.magenta('@@ -1 +1 @@'))

    if (opts.keys) {
      keys.forEach(([k, v]) => {
        if (v[0] != null) console.log(clc.red(`- ${k}\t${v[0]}`))
        if (v[1] != null) console.log(clc.green(`+ ${k}\t${v[1]}`))
      })
    } else {
      additions.forEach(b => console.log(clc.green(`+ ${b.cid}`)))
      removals.forEach(b => console.log(clc.red(`- ${b.cid}`)))
    }
  })

cli.command('merge <path>')
  .describe('Merge the passed pail into this pail.')
  .action(async (path, opts) => {
    const [
      { root: aroot, blocks: ablocks },
      { root: broot, blocks: bblocks }
    ] = await Promise.all([openPail(opts.path), openPail(path)])
    if (aroot.toString() === broot.toString()) return

    const fetcher = new MultiBlockFetcher(ablocks, bblocks)
    const { root, additions, removals } = await merge(fetcher, aroot, [broot])

    await updatePail(opts.path, ablocks, root, { additions, removals })

    console.log(clc.red(`--- ${aroot}`))
    console.log(clc.green(`+++ ${root}`))
    console.log(clc.magenta('@@ -1 +1 @@'))
    additions.forEach(b => console.log(clc.green(`+ ${b.cid}`)))
    removals.forEach(b => console.log(clc.red(`- ${b.cid}`)))
  })

cli.parse(process.argv)

/**
 * @param {string} path
 * @returns {Promise<{ root: API.ShardLink, blocks: MemoryBlockstore }>}
 */
async function openPail (path) {
  const blocks = new MemoryBlockstore()
  try {
    const carReader = new CARReaderStream()
    const readable = /** @type {ReadableStream<Uint8Array>} */ (Readable.toWeb(fs.createReadStream(path)))
    await readable.pipeThrough(carReader).pipeTo(new WritableStream({ write: b => blocks.put(b.cid, b.bytes) }))
    const header = await carReader.getHeader()
    if (!isShardLink(header.roots[0])) throw new Error(`not a shard: ${header.roots[0]}`)
    return { root: header.roots[0], blocks }
  } catch (err) {
    if (err.code !== 'ENOENT') throw new Error('failed to open bucket', { cause: err })
    const rootblk = await ShardBlock.create()
    blocks.put(rootblk.cid, rootblk.bytes)
    return { root: rootblk.cid, blocks }
  }
}

/**
 * @param {string} path
 * @param {MemoryBlockstore} blocks
 * @param {API.ShardLink} root
 * @param {API.ShardDiff} diff
 */
async function updatePail (path, blocks, root, { additions, removals }) {
  const tmp = join(os.tmpdir(), `pail${Date.now()}.car`)
  const iterator = blocks.entries()
  const readable = new ReadableStream({
    start (controller) {
      for (const b of additions) controller.enqueue(b)
    },
    pull (controller) {
      for (const b of iterator) {
        if (removals.some(r => b.cid.toString() === r.cid.toString())) continue
        return controller.enqueue(b)
      }
      controller.close()
    }
  })
  await readable.pipeThrough(new CARWriterStream([root])).pipeTo(Writable.toWeb(fs.createWriteStream(tmp)))

  const old = `${path}-${new Date().toISOString()}`
  try {
    await fs.promises.rename(path, old)
  } catch (err) {
    if (err.code !== 'ENOENT') throw err
  }
  await fs.promises.rename(tmp, path)
  try {
    await fs.promises.rm(old)
  } catch (err) {
    if (err.code !== 'ENOENT') throw err
  }
}
