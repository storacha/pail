import { describe, it } from 'mocha'
import assert from 'node:assert'
import { Clock, EventBlock, vis } from '../clock.js'
import { Blockstore, randomCID } from './helpers.js'

describe('clock', () => {
  it('create a new clock', async () => {
    const blocks = new Blockstore()
    const clock = new Clock(blocks)
    assert.equal(clock.head.length, 0)

    const dataCID = await randomCID(32)
    const event = await EventBlock.create(dataCID)

    await blocks.put(event.cid, event.bytes)
    await clock.advance(event.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 1)
    assert.equal(clock.head[0].toString(), event.cid.toString())
  })

  it('add an event', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomCID(32))
    await blocks.put(root.cid, root.bytes)

    const clock = new Clock(blocks, [root.cid])

    const event = await EventBlock.create(await randomCID(32), clock.head)
    await blocks.put(event.cid, event.bytes)

    await clock.advance(event.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 1)
    assert.equal(clock.head[0].toString(), event.cid.toString())
  })

  it('add two events with shared parents', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomCID(32))
    await blocks.put(root.cid, root.bytes)

    const clock = new Clock(blocks, [root.cid])
    const parents = clock.head

    const event0 = await EventBlock.create(await randomCID(32), parents)
    await blocks.put(event0.cid, event0.bytes)
    await clock.advance(event0.cid)

    const event1 = await EventBlock.create(await randomCID(32), parents)
    await blocks.put(event1.cid, event1.bytes)
    await clock.advance(event1.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 2)
    assert.equal(clock.head[0].toString(), event0.cid.toString())
    assert.equal(clock.head[1].toString(), event1.cid.toString())
  })

  it('add two events with some shared parents', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomCID(32))
    await blocks.put(root.cid, root.bytes)

    const clock = new Clock(blocks, [root.cid])
    const parents0 = clock.head

    const event0 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event0.cid, event0.bytes)
    await clock.advance(event0.cid)

    const event1 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event1.cid, event1.bytes)
    await clock.advance(event1.cid)

    const event2 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event2.cid, event2.bytes)
    await clock.advance(event2.cid)

    const event3 = await EventBlock.create(await randomCID(32), [event0.cid, event1.cid])
    await blocks.put(event3.cid, event3.bytes)
    await clock.advance(event3.cid)

    const event4 = await EventBlock.create(await randomCID(32), [event2.cid])
    await blocks.put(event4.cid, event4.bytes)
    await clock.advance(event4.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 2)
    assert.equal(clock.head[0].toString(), event3.cid.toString())
    assert.equal(clock.head[1].toString(), event4.cid.toString())
  })

  it('converge when multi-root', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomCID(32))
    await blocks.put(root.cid, root.bytes)

    const clock = new Clock(blocks, [root.cid])
    const parents0 = clock.head

    const event0 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event0.cid, event0.bytes)
    await clock.advance(event0.cid)

    const event1 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event1.cid, event1.bytes)
    await clock.advance(event1.cid)

    const parents1 = clock.head

    const event2 = await EventBlock.create(await randomCID(32), parents1)
    await blocks.put(event2.cid, event2.bytes)
    await clock.advance(event2.cid)

    const event3 = await EventBlock.create(await randomCID(32), parents1)
    await blocks.put(event3.cid, event3.bytes)
    await clock.advance(event3.cid)

    const event4 = await EventBlock.create(await randomCID(32), parents1)
    await blocks.put(event4.cid, event4.bytes)
    await clock.advance(event4.cid)

    const parents2 = clock.head

    const event5 = await EventBlock.create(await randomCID(32), parents2)
    await blocks.put(event5.cid, event5.bytes)
    await clock.advance(event5.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 1)
    assert.equal(clock.head[0].toString(), event5.cid.toString())
  })

  it('add an old event', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomCID(32))
    await blocks.put(root.cid, root.bytes)

    const clock = new Clock(blocks, [root.cid])
    const parents0 = clock.head

    const event0 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event0.cid, event0.bytes)
    await clock.advance(event0.cid)

    const event1 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event1.cid, event1.bytes)
    await clock.advance(event1.cid)

    const parents1 = clock.head

    const event2 = await EventBlock.create(await randomCID(32), parents1)
    await blocks.put(event2.cid, event2.bytes)
    await clock.advance(event2.cid)

    const event3 = await EventBlock.create(await randomCID(32), parents1)
    await blocks.put(event3.cid, event3.bytes)
    await clock.advance(event3.cid)

    const event4 = await EventBlock.create(await randomCID(32), parents1)
    await blocks.put(event4.cid, event4.bytes)
    await clock.advance(event4.cid)

    const parents2 = clock.head

    const event5 = await EventBlock.create(await randomCID(32), parents2)
    await blocks.put(event5.cid, event5.bytes)
    await clock.advance(event5.cid)

    // now very old one
    const event6 = await EventBlock.create(await randomCID(32), parents0)
    await blocks.put(event6.cid, event6.bytes)
    await clock.advance(event6.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 2)
    assert.equal(clock.head[0].toString(), event5.cid.toString())
    assert.equal(clock.head[1].toString(), event6.cid.toString())
  })

  it('add an event with missing parents', async () => {
    const blocks = new Blockstore()
    const root = await EventBlock.create(await randomCID(32))
    await blocks.put(root.cid, root.bytes)

    const clock = new Clock(blocks, [root.cid])

    const event0 = await EventBlock.create(await randomCID(32), clock.head)
    await blocks.put(event0.cid, event0.bytes)

    const event1 = await EventBlock.create(await randomCID(32), [event0.cid])
    await blocks.put(event1.cid, event1.bytes)

    await clock.advance(event1.cid)

    for await (const line of vis(blocks, clock.head)) console.log(line)
    assert.equal(clock.head.length, 1)
    assert.equal(clock.head[0].toString(), event1.cid.toString())
  })
})
