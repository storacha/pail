import { Link, UnknownLink, BlockView, Block, Version } from 'multiformats'
import { sha256 } from 'multiformats/hashes/sha2'
import * as dagCBOR from '@ipld/dag-cbor'

export { Link, UnknownLink, BlockView, Block, Version }

export type ShardEntryValueValue = UnknownLink

export type ShardEntryLinkValue = [ShardLink]

export type ShardEntryLinkAndValueValue = [ShardLink, UnknownLink]

export type ShardValueEntry = [key: string, value: ShardEntryValueValue]

export type ShardLinkEntry = [key: string, value: ShardEntryLinkValue | ShardEntryLinkAndValueValue]

/** Single key/value entry within a shard. */
export type ShardEntry = [key: string, value: ShardEntryValueValue | ShardEntryLinkValue | ShardEntryLinkAndValueValue]

export interface Shard {
  entries: ShardEntry[]
  /** Max key length (in UTF-8 encoded characters) - default 64. */
  maxKeyLength: number
  /** Max encoded shard size in bytes - default 512 KiB. */
  maxSize: number
}

export type ShardLink = Link<Shard, typeof dagCBOR.code, typeof sha256.code, 1>

export interface ShardBlockView extends BlockView<Shard, typeof dagCBOR.code, typeof sha256.code, 1> {
  prefix: string
}

export interface ShardDiff {
  additions: ShardBlockView[]
  removals: ShardBlockView[]
}

export interface BlockFetcher {
  get<T = unknown, C extends number = number, A extends number = number, V extends Version = 1> (link: Link<T, C, A, V>):
    Promise<Block<T, C, A, V> | undefined>
}

export interface ShardConfig {
  maxSize: number
  maxKeyLength: number
}

export type ShardOptions = Partial<ShardConfig>

export interface Batcher {
  put (key: string, value: UnknownLink): Promise<void>
  // del (key: string): Promise<void>
  commit (): Promise<{ root: ShardLink } & ShardDiff>
}

export interface Operation {
  type: 'put',
  key: string
  value: UnknownLink
}

// Clock //////////////////////////////////////////////////////////////////////

export type EventLink<T> = Link<EventView<T>>

export interface EventView<T> {
  parents: EventLink<T>[]
  data: T
}

export interface EventBlockView<T> extends BlockView<EventView<T>> {}

// CRDT ///////////////////////////////////////////////////////////////////////

export interface EventData {
  type: 'put'|'del'
  key: string
  value: UnknownLink
  root: ShardLink
}
