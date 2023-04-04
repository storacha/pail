import Fireproof from './fireproof.js'
import DbIndex from './db-index.js'

export function fromJSON (json, blocks) {
  const fp = new Fireproof(blocks, json.clock, { name: json.name })
  for (const index of json.indexes) {
    DbIndex.fromJSON(fp, index)
  }
  return fp
}
