import { describe, it } from 'mocha'
import assert from 'node:assert'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/clock/api.js'
import { advance, EventBlock, vis } from '../src/clock/index.js'
import { Blockstore, randomCID } from './helpers.js'

async function randomEventData () {
  return {
    type: 'put',
    key: `test-${Date.now()}`,
    value: await randomCID(32)
  }
}

describe('clock', () => {
  it('create a new clock', async () => {
    const blocks = new Blockstore()
    const event = await EventBlock.create({})

    await blocks.put(event.cid, event.bytes)
    const head = await advance(blocks, [], event.cid)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 1)
    assert.equal(head[0].toString(), event.cid.toString())
  })

  it('add an event', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomEventData())
    await blocks.put(root.cid, root.bytes)

    /** @type {API.EventLink<any>[]} */
    let head = [root.cid]

    const event = await EventBlock.create(await randomEventData(), head)
    await blocks.put(event.cid, event.bytes)

    head = await advance(blocks, head, event.cid)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 1)
    assert.equal(head[0].toString(), event.cid.toString())
  })

  it('add two events with shared parents', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomEventData())
    await blocks.put(root.cid, root.bytes)

    /** @type {API.EventLink<any>[]} */
    let head = [root.cid]
    const parents = head

    const event0 = await EventBlock.create(await randomEventData(), parents)
    await blocks.put(event0.cid, event0.bytes)
    head = await advance(blocks, parents, event0.cid)

    const event1 = await EventBlock.create(await randomEventData(), parents)
    await blocks.put(event1.cid, event1.bytes)
    head = await advance(blocks, head, event1.cid)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 2)
    assert.equal(head[0].toString(), event0.cid.toString())
    assert.equal(head[1].toString(), event1.cid.toString())
  })

  it('add two events with some shared parents', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomEventData())
    await blocks.put(root.cid, root.bytes)

    /** @type {API.EventLink<any>[]} */
    let head = [root.cid]
    const parents0 = head

    const event0 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event0.cid, event0.bytes)
    head = await advance(blocks, head, event0.cid)

    const event1 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event1.cid, event1.bytes)
    head = await advance(blocks, head, event1.cid)

    const event2 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event2.cid, event2.bytes)
    head = await advance(blocks, head, event2.cid)

    const event3 = await EventBlock.create(await randomEventData(), [event0.cid, event1.cid])
    await blocks.put(event3.cid, event3.bytes)
    head = await advance(blocks, head, event3.cid)

    const event4 = await EventBlock.create(await randomEventData(), [event2.cid])
    await blocks.put(event4.cid, event4.bytes)
    head = await advance(blocks, head, event4.cid)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 2)
    assert.equal(head[0].toString(), event3.cid.toString())
    assert.equal(head[1].toString(), event4.cid.toString())
  })

  it('converge when multi-root', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomEventData())
    await blocks.put(root.cid, root.bytes)

    /** @type {API.EventLink<any>[]} */
    let head = [root.cid]
    const parents0 = head

    const event0 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event0.cid, event0.bytes)
    head = await advance(blocks, head, event0.cid)

    const event1 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event1.cid, event1.bytes)
    head = await advance(blocks, head, event1.cid)

    const parents1 = head

    const event2 = await EventBlock.create(await randomEventData(), parents1)
    await blocks.put(event2.cid, event2.bytes)
    head = await advance(blocks, head, event2.cid)

    const event3 = await EventBlock.create(await randomEventData(), parents1)
    await blocks.put(event3.cid, event3.bytes)
    head = await advance(blocks, head, event3.cid)

    const event4 = await EventBlock.create(await randomEventData(), parents1)
    await blocks.put(event4.cid, event4.bytes)
    head = await advance(blocks, head, event4.cid)

    const parents2 = head

    const event5 = await EventBlock.create(await randomEventData(), parents2)
    await blocks.put(event5.cid, event5.bytes)
    head = await advance(blocks, head, event5.cid)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 1)
    assert.equal(head[0].toString(), event5.cid.toString())
  })

  it('add an old event', async () => {
    const blocks = new Blockstore()
    const blockGet = blocks.get.bind(blocks)
    let count = 0
    blocks.get = async cid => {
      count++
      return blockGet(cid)
    }
    const root = await EventBlock.create(await randomEventData())
    await blocks.put(root.cid, root.bytes)

    /** @type {API.EventLink<any>[]} */
    let head = [root.cid]
    const parents0 = head

    const event0 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event0.cid, event0.bytes)
    head = await advance(blocks, head, event0.cid)

    const event1 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event1.cid, event1.bytes)
    head = await advance(blocks, head, event1.cid)

    const parents1 = head

    const event2 = await EventBlock.create(await randomEventData(), parents1)
    await blocks.put(event2.cid, event2.bytes)
    head = await advance(blocks, head, event2.cid)

    const event3 = await EventBlock.create(await randomEventData(), parents1)
    await blocks.put(event3.cid, event3.bytes)
    head = await advance(blocks, head, event3.cid)

    const event4 = await EventBlock.create(await randomEventData(), parents1)
    await blocks.put(event4.cid, event4.bytes)
    head = await advance(blocks, head, event4.cid)

    const parents2 = head

    const event5 = await EventBlock.create(await randomEventData(), parents2)
    await blocks.put(event5.cid, event5.bytes)
    head = await advance(blocks, head, event5.cid)

    // now very old one
    const event6 = await EventBlock.create(await randomEventData(), parents0)
    await blocks.put(event6.cid, event6.bytes)
    const before = count
    head = await advance(blocks, head, event6.cid)
    assert.equal(count - before, 10)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 2)
    assert.equal(head[0].toString(), event5.cid.toString())
    assert.equal(head[1].toString(), event6.cid.toString())
  })

  it('add an event with missing parents', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomEventData())
    await blocks.put(root.cid, root.bytes)

    /** @type {API.EventLink<any>[]} */
    let head = [root.cid]

    const event0 = await EventBlock.create(await randomEventData(), head)
    await blocks.put(event0.cid, event0.bytes)

    const event1 = await EventBlock.create(await randomEventData(), [event0.cid])
    await blocks.put(event1.cid, event1.bytes)

    head = await advance(blocks, head, event1.cid)

    for await (const line of vis(blocks, head)) console.log(line)
    assert.equal(head.length, 1)
    assert.equal(head[0].toString(), event1.cid.toString())
  })


  /*
  ```mermaid
  flowchart TB
      event3 --> event1
      event3 --> event2
      event2 --> event0
      event1 --> event0
      event0 --> genesis
      event4 --> genesis
  ```

  All we want to do is prove that `event0` and `genesis` are not fetched
  multiple times, since there are multiple paths to it in the tree.

  We arrive at 8, because when we advance with `head: [event3], event: event4`
  we firstly check if the head is in the event:

  * get event4 (1)
  * get event3 (2)
  * get genesis (3)

  Then we check if the event is in the head, with de-duping:

  * get event3 (4)
  * get event4 (5)
  * then get each of the nodes back to parent(s) of `event4` (`genesis`):
      * event1 (6)
      * event2 (7)
      * event0 (8)
      * (we don't fetch genesis due to existing cycle detection)

  Without deduping, we expect 9 node fetches, since we traverse across `event0`
  again, since it is linked to by 2 nodes. 
   */
  it('contains only traverses history once', async () => {
    const blocks = new Blockstore()
    const genesis = await EventBlock.create(await randomEventData())
    await blocks.put(genesis.cid, genesis.bytes)
    /** @type {API.EventLink<any>[]} */
    let head = [genesis.cid]
    const blockGet = blocks.get.bind(blocks)
    let count = 0
    blocks.get = async cid => {
      count++
      return blockGet(cid)
    }

    const event0 = await EventBlock.create(await randomEventData(), [genesis.cid])
    await blocks.put(event0.cid, event0.bytes)
    head = await advance(blocks, head, event0.cid)

    const event1 = await EventBlock.create(await randomEventData(), [event0.cid])
    await blocks.put(event1.cid, event1.bytes)
    head = await advance(blocks, head, event1.cid)

    const event2 = await EventBlock.create(await randomEventData(), [event0.cid])
    await blocks.put(event2.cid, event2.bytes)
    head = await advance(blocks, head, event2.cid)

    const event3 = await EventBlock.create(await randomEventData(), [event1.cid, event2.cid])
    await blocks.put(event3.cid, event3.bytes)
    head = await advance(blocks, head, event3.cid)

    const before = count
    const event4 = await EventBlock.create(await randomEventData(), [genesis.cid])
    await blocks.put(event4.cid, event4.bytes)
    head = await advance(blocks, head, event4.cid)

    assert.equal(head.length, 2)
    assert.equal(head[1].toString(), event4.cid.toString())
    assert.equal(head[0].toString(), event3.cid.toString())
    assert.equal(count - before, 8, 'The number of traversals should be 8 with optimization')
  })

  it('sorts event links', async () => {
    const parent0 = await EventBlock.create(await randomEventData())
    const parent1 = await EventBlock.create(await randomEventData())
    const child0 = await EventBlock.create({}, [parent0.cid, parent1.cid])
    const child1 = await EventBlock.create({}, [parent1.cid, parent0.cid])
    assert.deepEqual(child0.bytes, child1.bytes)
  })
})
