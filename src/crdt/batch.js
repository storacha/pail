// eslint-disable-next-line no-unused-vars
import * as API from '../api.js'
import * as Batch from '../batch.js'

/**
 * @param {API.BlockFetcher} blocks Bucket block storage.
 * @param {API.ShardLink} root CID of the root shard block.
 * @returns {Promise<API.Batcher>}
 */
export const create = async (blocks, root) => {
  const batch = await Batch.create(blocks, root)
  return batch
}
