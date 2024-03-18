import {
  Batcher,
  BatcherShardEntry,
  ShardBlockView,
  BlockFetcher,
  ShardLink,
  UnknownLink
} from '../../batch/api.js'
import { Operation, BatchOperation, EventLink, Result } from '../api.js'

export {
  Batcher,
  BatcherShardEntry,
  ShardBlockView,
  BlockFetcher,
  ShardLink,
  UnknownLink,
  Operation,
  BatchOperation,
  EventLink,
  Result
}

export interface CRDTBatcher extends Batcher {
  /**
   * Encode all altered shards in the batch and return the new root CID, new
   * clock head, the new clock event and the difference blocks.
   */
  commit (): Promise<Result>
}
