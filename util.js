const BSON = require('bson-ext')
const Long = BSON.Long
const _ = require('lodash')

exports.describeTypes = function (doc) {
  if (doc._bsontype) {
    return doc._bsontype
  } else if (Array.isArray(doc)) {
    const types = doc.map(exports.describeTypes)
    const uniqueTypes = _.uniqWith(types, _.isEqual)
    if (uniqueTypes.length > 1) {
      throw new Error('arrays containing multiple data types are not allowed')
    }
    if (_.isUndefined(uniqueTypes[0])) {
      return []
    }
    return uniqueTypes
  } else if (typeof doc === 'object') {
    const out = {}
    for (const key of Object.keys(doc)) {
      const v = exports.describeTypes(doc[key])
      if (typeof v !== 'undefined') {
        out[key] = v
      }
    }
    return out
  }
}

exports.createCursor = function (ns, firstBatch, id = Long.fromNumber(0)) {
  return {
    cursor: {
      id,
      ns,
      firstBatch
    },
    ok: 1
  }
}

exports.listIndicesQuery = function (fieldName, collectionName) {
  return `select
    t.relname as table_name,
    i.relname as index_name,
    a.attname as column_name
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and t.relkind = 'r'
    and t.relname = '${collectionName}'
    and a.attname = '${fieldName}'
order by
    t.relname,
    i.relname;`
}
