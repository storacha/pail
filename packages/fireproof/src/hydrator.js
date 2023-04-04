import DbIndex from './db-index.js'
import Fireproof from './fireproof.js'
import { CID } from 'multiformats'

export default class Hydrator {
  static fromJSON (json, database) {
    // console.log('hydrating', json, database, database.indexes)
    database.hydrate({ clock: json.clock.map(c => CID.parse(c)), name: json.name })
    for (const { code, clock: { byId, byKey, db } } of json.indexes) {
      DbIndex.fromJSON(database, {
        clock: {
          byId: byId ? CID.parse(byId) : null,
          byKey: byKey ? CID.parse(byKey) : null,
          db: db ? db.map(c => CID.parse(c)) : []
        },
        code
      })
    }
    return database
  }

  static snapshot (database) {
    const definition = database.toJSON()
    const withBlocks = new Fireproof(database.blocks)
    const snappedDb = this.fromJSON(definition, withBlocks)
    ;([...database.indexes.values()]).forEach(index => {
      snappedDb.indexes.get(index.mapFnString).mapFn = index.mapFn
    })
    return snappedDb
  }
}
