import {
  UnknownLink,
  ShardLink,
  ShardDiff,
  ShardEntry,
  ShardEntryValueValue,
  ShardEntryLinkValue,
  ShardEntryLinkAndValueValue,
  ShardConfig,
  ShardOptions,
  ShardBlockView,
  BlockFetcher
} from '../api.js'

export {
  UnknownLink,
  ShardLink,
  ShardDiff,
  ShardEntry,
  ShardEntryValueValue,
  ShardEntryLinkValue,
  ShardEntryLinkAndValueValue,
  ShardConfig,
  ShardOptions,
  ShardBlockView,
  BlockFetcher
}

export interface BatcherShard extends ShardConfig {
  base?: ShardBlockView
  entries: BatcherShardEntry[]
}

export interface BatcherShardInit extends ShardOptions {
  base?: ShardBlockView
  entries?: BatcherShardEntry[]
}

export type BatcherShardEntry = [
  key: string,
  value: ShardEntryValueValue | ShardEntryLinkValue | ShardEntryLinkAndValueValue | ShardEntryShardValue | ShardEntryShardAndValueValue
]

export type ShardEntryShardValue = [BatcherShard]

export type ShardEntryShardAndValueValue = [BatcherShard, UnknownLink]

export interface Batcher {
  /**
   * Put a value (a CID) for the given key. If the key exists it's value is
   * overwritten.
   */
  put (key: string, value: UnknownLink): Promise<void>
  /**
   * Encode all altered shards in the batch and return the new root CID and
   * difference blocks.
   */
  commit (): Promise<{ root: ShardLink } & ShardDiff>
}
