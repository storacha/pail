import { ShardDiff, ShardLink, UnknownLink } from '../api.js'
import { EventLink, EventBlockView } from '../clock/api.js'

export { BlockFetcher, UnknownLink, ShardBlockView, ShardDiff, ShardLink, EntriesOptions } from '../api.js'
export { EventBlockView, EventLink } from '../clock/api.js'

export interface Result extends ShardDiff {
  root: ShardLink
  head: EventLink<Operation>[]
  event?: EventBlockView<Operation>
}

export type Operation = (
  | PutOperation
  | DeleteOperation
  | BatchOperation
) & { root: ShardLink }

export interface PutOperation {
  type: 'put',
  key: string
  value: UnknownLink
}

export interface DeleteOperation {
  type: 'del',
  key: string
}

export interface BatchOperation {
  type: 'batch',
  ops: Array<PutOperation|DeleteOperation>
}
