import { Link, BlockView } from 'multiformats'

export { BlockFetcher } from '../api.js'

export type EventLink<T> = Link<EventView<T>>

export interface EventView<T> {
  parents: EventLink<T>[]
  data: T
}

export interface EventBlockView<T> extends BlockView<EventView<T>> {}
