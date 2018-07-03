const assert = require('chai').assert
const BSON = require('bson-ext')
const util =  require('../util')
const describeTypes = util.describeTypes
const getArrayPaths = util.getArrayPaths

describe('util: describeTypes', function() {
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

describe('util: getArrayPaths', function() {
  it('arrays', function () {
    assert.deepEqual(getArrayPaths({ arr: [1, 2] }), ['arr'] )
  })
  it('multiple arrays', function () {
    assert.deepEqual(getArrayPaths({ arr: [1, 2], arr2: ['1'] }), ['arr', 'arr2'] )
  })
  it('deep array', function () {
    assert.deepEqual(getArrayPaths({ deep: { arr: [1, 2] } }), ['deep.arr'] )
  })
  it('array of ids', function () {
    assert.deepEqual(getArrayPaths({ arr: [BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c30'), BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c32')] }), ['arr'])
  })
  it('object with mixed', function () {
    assert.deepEqual(getArrayPaths({ a: '123', id2: BSON.ObjectID('5b1e1a6fa00fd75c4e6c4c30') }), [] )
  })
})
