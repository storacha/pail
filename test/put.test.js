import assert from 'node:assert'
// eslint-disable-next-line no-unused-vars
import * as API from '../src/api.js'
import { put } from '../src/index.js'
import { ShardBlock } from '../src/shard.js'
import { Blockstore, materialize, putAll, randomCID, randomInteger, randomString, verify } from './helpers.js'

describe('put', () => {
  it('put to empty shard', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)
    const result = await put(blocks, root.cid, 'test', dataCID)

    assert.equal(result.removals.length, 1)
    assert.equal(result.removals[0].cid.toString(), root.cid.toString())
    assert.equal(result.additions.length, 1)
    assert.equal(result.additions[0].value.entries.length, 1)
    assert.equal(result.additions[0].value.entries[0][0], 'test')
    assert.equal(result.additions[0].value.entries[0][1].toString(), dataCID.toString())
  })

  it('put same value to existing key', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)
    const result0 = await put(blocks, root.cid, 'test', dataCID)

    assert.equal(result0.removals.length, 1)
    assert.equal(result0.removals[0].cid.toString(), root.cid.toString())
    assert.equal(result0.additions.length, 1)
    assert.equal(result0.additions[0].value.entries.length, 1)
    assert.equal(result0.additions[0].value.entries[0][0], 'test')
    assert.equal(result0.additions[0].value.entries[0][1].toString(), dataCID.toString())

    for (const b of result0.additions) {
      await blocks.put(b.cid, b.bytes)
    }

    const result1 = await put(blocks, result0.root, 'test', dataCID)

    assert.equal(result1.removals.length, 0)
    assert.equal(result1.additions.length, 0)
    assert.equal(result1.root.toString(), result0.root.toString())
  })

  it('auto-shards on similar key', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)

    const res = await putAll(blocks, root.cid, [
      ['aaaa', dataCID],
      ['aabb', dataCID]
    ])

    assert.deepEqual(
      await materialize(blocks, res.root),
      [
        [
          'a',
          [
            [
              [
                'a',
                [
                  [
                    ['aa', dataCID],
                    ['bb', dataCID]
                  ]
                ]
              ]
            ]
          ]
        ]
      ]
    )
  })

  it('put to shard link', async () => {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    const dataCID = await randomCID(32)

    const res = await putAll(blocks, root.cid, [
      ['aaaa', dataCID],
      ['aabb', dataCID],
      ['aa', dataCID]
    ])

    assert.deepEqual(
      await materialize(blocks, res.root),
      [
        [
          'a',
          [
            [
              [
                'a',
                [
                  [
                    ['aa', dataCID],
                    ['bb', dataCID]
                  ],
                  dataCID
                ]
              ]
            ]
          ]
        ]
      ]
    )
  })

  it('deterministic structure', async () => {
    const dataCID = await randomCID(32)
    /** @type {Array<[string, API.UnknownLink]>} */
    const items = [
      ['aaaa', dataCID],
      ['aaab', dataCID],
      ['aabb', dataCID],
      ['abbb', dataCID],
      ['bbbb', dataCID]
    ]
    const orders = [
      [0, 1, 2, 3, 4],
      [4, 3, 2, 1, 0],
      [1, 2, 4, 0, 3],
      [2, 0, 3, 4, 1]
    ]
    for (const order of orders) {
      const root = await ShardBlock.create()
      const blocks = new Blockstore()
      await blocks.put(root.cid, root.bytes)

      const res = await putAll(blocks, root.cid, order.map(i => items[i]))

      assert.deepEqual(
        await materialize(blocks, res.root),
        [
          [
            'a',
            [
              [
                [
                  'a',
                  [
                    [
                      [
                        'a',
                        [
                          [
                            ['a', dataCID],
                            ['b', dataCID]
                          ]
                        ]
                      ],
                      ['bb', dataCID]
                    ]
                  ]
                ],
                ['bbb', dataCID]
              ]
            ]
          ],
          ['bbbb', dataCID]
        ]
      )
    }
  })

  it('put 10,000x', async function () {
    const root = await ShardBlock.create()
    const blocks = new Blockstore()
    await blocks.put(root.cid, root.bytes)

    /** @type {Array<[string, API.UnknownLink]>} */
    const items = []
    for (let i = 0; i < 10_000; i++) {
      const k = randomString(randomInteger(1, 64))
      const v = await randomCID(randomInteger(8, 128))
      items.push([k, v])
    }

    const res = await putAll(blocks, root.cid, items)
    await assert.doesNotReject(verify(blocks, res.root, new Map(items)))
  }, 10_000)
})
