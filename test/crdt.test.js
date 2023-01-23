import { describe, it } from 'mocha'
import assert from 'node:assert'
import { MultiBlockFetcher } from '../block.js'
import { advance, vis } from '../clock.js'
import { put } from '../crdt.js'
import { Blockstore, randomCID } from './helpers.js'

describe('CRDT', () => {
  it('put a value to a new clock', async () => {
    const blocks = new Blockstore()
    const key = 'test'
    const value = await randomCID(32)
    const { event, head } = await putAndVis(blocks, [], key, value)

    assert.equal(event.value.data.type, 'put')
    assert.equal(event.value.data.key, key)
    assert.equal(event.value.data.value.toString(), value.toString())
    assert.equal(head.length, 1)
    assert.equal(head[0].toString(), event.cid.toString())
  })

  it('linear put multiple values', async () => {
    const blocks = new Blockstore()
    const key0 = 'test'
    const value0 = await randomCID(32)
    const result0 = await put(blocks, [], key0, value0)

    blocks.putSync(result0.event.cid, result0.event.bytes)
    for (const a of result0.additions) blocks.putSync(a.cid, a.bytes)

    const key1 = 'test1'
    const value1 = await randomCID(32)
    const result1 = await putAndVis(blocks, result0.head, key1, value1)

    assert.equal(result1.event.value.data.type, 'put')
    assert.equal(result1.event.value.data.key, key1)
    assert.equal(result1.event.value.data.value.toString(), value1.toString())
    assert.equal(result1.head.length, 1)
    assert.equal(result1.head[0].toString(), result1.event.cid.toString())
  })

  it('simple parallel put multiple values', async () => {
    const blocks = new Blockstore()
    const key0 = 'test'
    const value0 = await randomCID(32)
    const result0 = await put(blocks, [], key0, value0)

    blocks.putSync(result0.event.cid, result0.event.bytes)
    for (const a of result0.additions) blocks.putSync(a.cid, a.bytes)

    const key1 = 'test1'
    const value1 = await randomCID(32)
    const result1 = await put(blocks, result0.head, key1, value1)

    blocks.putSync(result1.event.cid, result1.event.bytes)
    for (const a of result1.additions) blocks.putSync(a.cid, a.bytes)

    const key2 = 'test2'
    const value2 = await randomCID(32)
    const result2 = await put(blocks, result0.head, key2, value2)

    blocks.putSync(result2.event.cid, result2.event.bytes)
    for (const a of result2.additions) blocks.putSync(a.cid, a.bytes)

    const head1 = await advance(blocks, result1.head, result2.event.cid)
    const key3 = 'test3'
    const value3 = await randomCID(32)
    const result3 = await putAndVis(blocks, head1, key3, value3)

    const head2 = await advance(blocks, result2.head, result1.event.cid)
    const result4 = await putAndVis(blocks, head2, key3, value3)

    assert.equal(result3.event.value.data.root.toString(), result4.event.value.data.root.toString())
  })
})

/**
 * @param {import('../block.js').BlockFetcher} blocks
 * @param {import('../clock.js').EventLink<import('../crdt.js').EventData>[]} head
 * @param {string} key
 * @param {import('../link.js').AnyLink} value
 */
async function putAndVis (blocks, head, key, value) {
  const result = await put(blocks, head, key, value)
  const rblocks = new Blockstore()
  rblocks.putSync(result.event.cid, result.event.bytes)
  for (const a of result.additions) {
    rblocks.putSync(a.cid, a.bytes)
  }
  for await (const line of vis(new MultiBlockFetcher(blocks, rblocks), result.head)) {
    console.log(line)
  }
  return result
}
