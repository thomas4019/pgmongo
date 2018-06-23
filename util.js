const BSON = require('bson-ext')
const bson = new BSON([BSON.Binary, BSON.Code, BSON.DBRef, BSON.Decimal128, BSON.Double, BSON.Int32, BSON.Long, BSON.Map, BSON.MaxKey, BSON.MinKey, BSON.ObjectId, BSON.BSONRegExp, BSON.Symbol, BSON.Timestamp])
const _ = require('lodash')

exports.describeTypes = function (doc) {
  if (doc._bsontype) {
    return doc._bsontype;
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