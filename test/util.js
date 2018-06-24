const assert = require('chai').assert
const BSON = require('bson-ext')
const util =  require('../util')
const describeTypes = util.describeTypes

describe('util: ', function() {
  it('basic type', function () {
    assert.equal(describeTypes(BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c30')), 'ObjectID')
  })
  it('returns undefined for strings', function () {
    assert.equal(describeTypes('123'), undefined)
  })
  it('object with no special values', function () {
    assert.deepEqual(describeTypes({ a: '123', b: 12 }), {})
  })
  it('arrays', function () {
    assert.deepEqual(describeTypes({ arr: [1, 2] }), { arr: [] })
  })
  it('array of ids', function () {
    assert.deepEqual(describeTypes({ arr: [BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c30'), BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c32')] }), { arr: ['ObjectID'] })
  })
  it('object with mixed', function () {
    assert.deepEqual(describeTypes({ a: '123', id2: BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c30') }), { id2: 'ObjectID'})
  })
})