import {
  UnknownLink,
  ShardLink,
  ShardDiff,
  ShardEntry,
  ShardEntryValueValue,
  ShardEntryLinkValue,
  ShardEntryLinkAndValueValue,
  ShardConfig,
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
  ShardBlockView,
  BlockFetcher
}

export interface BatcherShard {
  base?: ShardBlockView
  prefix: string
  entries: BatcherShardEntry<this>[]
  maxKeyLength: number
  maxSize: number
}

export type BatcherShardEntry<T extends BatcherShard> = [
  key: string,
  value: ShardEntryValueValue | ShardEntryLinkValue | ShardEntryLinkAndValueValue | ShardEntryShardValue<T> | ShardEntryShardAndValueValue<T>
]

export type ShardEntryShardValue<T> = [T]

export type ShardEntryShardAndValueValue<T> = [T, UnknownLink]

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
