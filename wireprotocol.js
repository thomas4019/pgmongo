const BSON = require('bson-ext')
const bson = new BSON([BSON.Binary, BSON.Code, BSON.DBRef, BSON.Decimal128, BSON.Double, BSON.Int32, BSON.Long, BSON.Map, BSON.MaxKey, BSON.MinKey, BSON.ObjectId, BSON.BSONRegExp, BSON.Symbol, BSON.Timestamp])
const debug = require('debug')

const OP_REPLY = 1
const OP_COMMANDREPLY = 2011

function getRandomInt(max) {
  max = max || Math.pow(2, 31) - 1
  return Math.floor(Math.random() * Math.floor(max))
}

exports.OP_QUERY = 2004
exports.OP_COMMAND = 2010

exports.parseHeader = function(buffer) {
  const opCode = buffer.readInt32LE(12)
  debug('pgmongo:opcode')(opCode)
  return {
    msgLength: buffer.readInt32LE(0),
    reqId: buffer.readInt32LE(4),
    opCode
  }
}

exports.parseQuery = function(buffer) {
  const flags = buffer.readInt32LE(16)
  const nameEnd = buffer.indexOf('\0', 20) + 1
  const collectionName = buffer.toString('utf8', 20, nameEnd - 1)
  const documents = []
  try {
    bson.deserializeStream(buffer, nameEnd + 8, 1, documents, 0)
  } catch (e) {
    console.error('error - missing or invalid body')
  }
  const doc = documents[0]
  return {
    flags,
    collectionName,
    doc
  }
}

exports.parseCommand = function(buffer) {
  const databaseEnd = buffer.indexOf('\0', 16) + 1
  const databaseName = buffer.toString('utf8', 16, databaseEnd - 1)
  const commandEnd = buffer.indexOf('\0', databaseEnd) + 1
  const commandName = buffer.toString('utf8', databaseEnd, commandEnd - 1).toLowerCase()
  const documents = []
  bson.deserializeStream(buffer, commandEnd, 2, documents, 0)
  const doc = documents[0]

  debug('pgmongo:indoc')(doc)
  return {
    databaseName,
    commandName,
    doc
  }
}

exports.createCommandReply = function(reqId, metadata, commandReply) {
  debug('pgmongo:reply')(metadata)
  if (metadata !== commandReply) {
    debug('pgmongo:reply2')(commandReply)
  }
  const metadataBuffer = bson.serialize(metadata)
  const replyBuffer = bson.serialize(commandReply)
  const length = 16 + metadataBuffer.length + replyBuffer.length

  const buf = Buffer.alloc(length)
  buf.writeInt32LE(length, 0)
  buf.writeInt32LE(getRandomInt(), 4)
  buf.writeInt32LE(reqId, 8)
  buf.writeInt32LE(OP_COMMANDREPLY, 12)

  metadataBuffer.copy(buf, 16)
  replyBuffer.copy(buf, 16 + metadataBuffer.length)

  return buf
}

exports.createResponse = function(reqId, doc) {
  debug('pgmongo:replyr')(doc)
  const docBuffer = bson.serialize(doc)
  const length = 36 + docBuffer.length

  const buf = Buffer.alloc(length)
  buf.writeInt32LE(length, 0)
  buf.writeInt32LE(0, 4)
  buf.writeInt32LE(reqId, 8)
  buf.writeInt32LE(OP_REPLY, 12)

  buf.writeInt32LE(0, 16)
  buf.writeInt32LE(0, 20)
  buf.writeInt32LE(0, 24)
  buf.writeInt32LE(0, 28)
  buf.writeInt32LE(1, 32)
  docBuffer.copy(buf, 36)

  return buf
}
