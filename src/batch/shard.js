// eslint-disable-next-line no-unused-vars
import * as API from './api.js'
import { configure } from '../shard.js'

/**
 * @param {API.BatcherShardInit} [init]
 * @returns {API.BatcherShard}
 */
export const create = init => ({
  base: init?.base,
  entries: [...init?.entries ?? []],
  ...configure(init)
})
