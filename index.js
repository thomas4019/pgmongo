#!/usr/bin/env node
const net = require('net')
const BSON = require('bson-ext')
const mongoToPostgres = require('mongo-query-to-postgres-jsonb')
const debug = require('debug')
const _ = require('lodash')

const util = require('./util')
const wire = require('./wireprotocol')
const adminReplies = require('./admin')

const args = process.argv.slice(2)
if (args.length < 1) {
  console.error('node index.js <database> [<pghost>] [<mongoport>]')
  return
}

const pgHost = args[1] || 'localhost'
const { Client } = require('pg')
const client = new Client({ database: args[0], host: pgHost })
client.connect()

let lastResult = {
  n: 0,
  connectionId : 1789,
  updatedExisting: true,
  errMsg: '',
  syncMillis: 0,
  writtenTo: null,
  ok: 1
}

async function doQuery(pgQuery) {
  debug('pgmongo:pgquery')(pgQuery)
  return await client.query(pgQuery)
}

async function createTable(collectionName) {
  await doQuery(`CREATE TABLE IF NOT EXISTS ${collectionName} (data jsonb)`)
}

async function tryOrCreateTable(action, collectionName) {
  try {
    return await action()
  } catch (e) {
    if (e.message.includes('does not exist')) {
      await createTable(collectionName)
      return await action()
    } else {
      throw e
    }
  }
}

var server = net.createServer(function (socket) {
  socket.on('data', (data) => processRecord(socket, data))
})

const convertToBSON = function(doc) {
  if (doc && doc._id && doc._id.length === 24) {
    doc._id = BSON.ObjectID(doc._id)
  }
  return doc
}

let commands = ['find', 'count', 'update', 'insert', 'create', 'delete', 'drop', 'validate',
  'listIndexes', 'createIndexes', 'deleteIndexes', 'renameCollection']

const arrayPathsMap = {}

function getArrayPaths(collectionName) {
  debug('pgmongo:arraypaths')(collectionName + ' ' + arrayPathsMap[collectionName])
  return arrayPathsMap[collectionName] || []
}

function safeWhereConversion(filter, collectionName) {
  try {
    return mongoToPostgres('data', filter, getArrayPaths(collectionName))
  } catch (err) {
    const data = {
      errmsg: 'in must be an array',
      code: 2,
      codeName: 'BadValue',
      ok: 0
    }
    console.error('query error')
    console.error(err)
    throw data
  }
}

async function crud(socket, reqId, databaseName, commandName, doc, build) {
  const normalizedCommandName = commandName.toLowerCase()
  let rawCollectionName = doc[commandName] || doc[normalizedCommandName]
  let collectionName = '"' + rawCollectionName + '"'
  if (commandName === 'distinct') {
    if ((doc.query && typeof doc.query !== 'object') || typeof doc.key !== 'string') {
      socket.write(build(reqId, { ok: 0, errmsg: '"query" had the wrong type. Expected object or null,', code: 14 }, {}))
      //throw new Error('\\"query\\" had the wrong type. Expected object or null, found ' + typeof doc.query)
      return true
    }
    const filter = doc.query || {}
    let where = safeWhereConversion(filter, rawCollectionName)
    const distinctField = mongoToPostgres.pathToText(['data'].concat(doc.key.split('.')), false)
    const arrayCondition = `jsonb_typeof(${distinctField})='array'`
    const query1 = `SELECT DISTINCT ${distinctField} AS data FROM ${collectionName} WHERE ${where} AND NOT ${arrayCondition}`
    const query2 = `SELECT DISTINCT jsonb_array_elements(${distinctField}) AS data FROM ${collectionName} WHERE ${where} AND ${arrayCondition}`
    const query = `${query1} UNION ${query2}`
    async function find() {
      return await doQuery(query)
    }
    res = await tryOrCreateTable(find, collectionName)
    const rows = res.rows.map((row) => convertToBSON(row.data))
    debug('pgmongo:rows')(rows)
    const data = { values: rows, ok: 1 }
    socket.write(build(reqId, data, {}))
    return true
  }
  if (commandName === 'find') {
    const filter = doc.filter
    const where = safeWhereConversion(filter, rawCollectionName)
    let select = mongoToPostgres.convertSelect('data', doc.projection)

    let query = `SELECT ${select} FROM ${collectionName}`
    if (where !== 'TRUE') {
      query += ` WHERE ${where}`
    }
    if (doc.sort) {
      query += ' ORDER BY ' + mongoToPostgres.convertSort('data', doc.sort, doc.collation && doc.collation.numericOrdering)
    }
    if (doc.limit) {
      query += ' LIMIT ' + Math.abs(doc.limit)
    }
    if (doc.skip) {
      if (doc.skip < 0) {
        socket.write(build(reqId, { ok: 0, errmsg: 'negative skip not allowed' }, {}))
        return true
      }
      query += ' OFFSET ' + doc.skip
    }
    let res
    try {
      async function find() {
        return await doQuery(query)
      }
      res = await tryOrCreateTable(find, collectionName)
    } catch (e) {
      console.error(e)
      res = { rows: [] }
    }
    const rows = res.rows.map((row) => convertToBSON(row.data))
    debug('pgmongo:rows')(rows)
    const data = util.createCursor(databaseName + '.' + collectionName, rows)
    socket.write(build(reqId, data, {}))
    return true
  }
  if (commandName === 'count') {
    if (doc.skip && doc.skip < 0) {
      socket.write(build(reqId, { ok: 0, errmsg: 'negative skip not allowed' }, {}))
      return true
    }
    const where = mongoToPostgres('data', doc.query, getArrayPaths(rawCollectionName))
    const data = { n: 0, ok: 1 }
    try {
      const res = await doQuery(`SELECT COUNT(*) FROM ${collectionName} WHERE ${where}`)
      data.n = res.rows[0].count
      if (doc.skip) {
        data.n = Math.max(0, data.n - doc.skip)
      }
      if (doc.limit) {
        data.n = Math.min(Math.abs(doc.limit), data.n)
      }
    } catch (e) {
      console.error(e)
    }
    socket.write(build(reqId, data, data))
    return true
  }
  if (commandName === 'update') {
    const update = doc.updates[0]
    const where = mongoToPostgres('data', update.q, getArrayPaths(rawCollectionName))
    let res
    try {
      const newValue = mongoToPostgres.convertUpdate('data', update.u)
      // TODO (handle multi)
      await createTable(collectionName)
      res = await doQuery(`UPDATE ${collectionName} SET data = ${newValue} WHERE ${where}`)
      if (res.rowCount === 0 && update.upsert) {
        const changes = update.u || {}
        if (mongoToPostgres.countUpdateSpecialKeys(changes) === 0) {
          // TODO: expand dot notation
          _.assign(changes, update.q)
        } else {
          changes['$set'] = changes['$set'] || {}
          _.assign(changes['$set'], update.q)
        }
        const newValue = mongoToPostgres.convertUpdate('data', changes, true)
        async function insert() {
          const res = await doQuery(`INSERT INTO ${collectionName} VALUES (${newValue})`)
          const data = { n: res.rowCount, nInserted: res.rowCount, updatedExisting: false, ok: 1 }
          socket.write(build(reqId, data, {}))
        }

        await tryOrCreateTable(insert, collectionName)
        return true
      } else {
        lastResult.n = res.rowCount
        socket.write(build(reqId, lastResult, lastResult))
        return true
      }
    } catch (e) {
      console.error(e)
      if (e.message.includes('_id')) {
        const data = {
          errmsg: e.message,
          ok: 0
        }
        socket.write(build(reqId, data, data))
        return true
      }
      // Can also upsert, fallback
      const isChangingId = typeof update.u._id !== 'undefined'
      if (isChangingId) {
        console.error('failed to update, falling back to insert')
        commandName = 'insert'
        doc.documents = [update.u]
      }
    }
  }
  if (commandName === 'insert') {
    arrayPathsMap[rawCollectionName] = _.union(arrayPathsMap[rawCollectionName], util.getArrayPaths(doc.documents[0]))
    const newValues = doc.documents.map((values) => '(' + mongoToPostgres.convertUpdate('data', values) + ')')
    async function insert() {
      const res = await doQuery(`INSERT INTO ${collectionName} VALUES ${newValues.join(',')}`)
      const data = { n: res.rowCount, nInserted: res.rowCount, updatedExisting: false, ok: 1 }
      socket.write(build(reqId, data, {}))
    }

    await tryOrCreateTable(insert, collectionName)
    return true
  }
  if (commandName === 'create') {
    lastResult.ok = 1
    await createTable(collectionName)
  }
  if (commandName === 'delete') {
    async function del() {
      const where = mongoToPostgres('data', doc.deletes[0].q, getArrayPaths(rawCollectionName))
      // TODO (handle multi)
      let query = `DELETE FROM ${collectionName} WHERE ${where}`
      if (doc.deletes[0].limit) {
        // TODO: handle limits on deletion
        //query += ' LIMIT ' + doc.deletes[0].limit
      }
      const res = await doQuery(query)
      lastResult.n = res.rowCount
      return socket.write(build(reqId, lastResult, lastResult))
    }
    await tryOrCreateTable(del, collectionName)
    return true
  }
  if (commandName === 'drop') {
    try {
      await doQuery(`DROP TABLE ${collectionName}`)
    } catch (e) {
      // often not an error if already doesn't exist
    }
    socket.write(build(reqId, lastResult, lastResult))
    return true
  }
  if (commandName === 'validate') {
    const countRes = await client.query(`SELECT COUNT(*) FROM ${collectionName}`)
    const data = {
      'ns' : databaseName + '.' + collectionName,
      'nrecords' : countRes.rows[0].count,
      'nIndexes' : 0,
      'keysPerIndex' : {},
      'valid' : true,
      'warnings' : ['Some checks omitted for speed. use {full:true} option to do more thorough scan.'],
      'errors' : [ ],
      'ok' : 1
    }
    return socket.write(build(reqId, data, data))
  }
  if (normalizedCommandName === 'listindexes') {
    const listIQuery = util.listIndicesQuery('data', rawCollectionName)
    const indexRes = await doQuery(listIQuery)
    const indices = indexRes.rows.map((row) => ({
      v: 2,
      key: { _id: 1 },
      name: row.index_name,
      ns: databaseName + '.' + collectionName
    }))
    const data = util.createCursor(databaseName + '.' + collectionName, indices)
    socket.write(build(reqId, data, data))
    return true
  }
  if (normalizedCommandName === 'createindexes') {
    rawCollectionName = rawCollectionName || doc['createIndexes']
    collectionName = '"' + rawCollectionName + '"'
    const data = {
      createIndexes: {
        numIndexesBefore: 0,
        numIndexesAfter: 0,
        ok: 1
      },
      ok: 1
    }
    const listIQuery = util.listIndicesQuery('data', rawCollectionName)
    const indexRes = await doQuery(listIQuery)
    const indices = indexRes.rows.map((row) => row.index_name)
    data.createIndexes.numIndexesBefore = indices.length
    for (const index of doc.indexes) {
      if (indices.includes(index.name)) {
        continue
      }
      const keys = Object.keys(index.key)
      /*if (keys.length > 1) {
        console.error(keys)
        throw new Error('compound indices not supported');
      }*/
      const pgPath = keys.map(function(key) {
        const path = ['data'].concat(key.split('.'))
        return mongoToPostgres.pathToText(path, false)
      }).join(', ')
      let indexQuery = `CREATE INDEX "${index.name}" ON ${collectionName} USING gin ((${pgPath}));`
      try {
        await doQuery(indexQuery)
      } catch (e) {
        console.error('failed to create index')
        //console.error(e)
      }
      data.createIndexes.numIndexesAfter++
    }
    if (normalizedCommandName === 'deleteindexes') {
      // Todo
      // Also handle case where index is "*" and all need to be dropped.
    }
    socket.write(build(reqId, data, data))
    return true
  }
  if (normalizedCommandName === 'renameCollection') {
    const data = { ok: 1 }
    collectionName = doc[commandName].split('.', 2)[1]
    const newName = doc.to.split('.', 2)[1]
    let query = `ALTER TABLE ${collectionName} RENAME TO "${newName}"`
    query = doc.dropTarget ? `DROP TABLE IF EXISTS ${newName}; ${query}` : query
    try {
      await doQuery(query)
    } catch (e) {
      data.ok = 0
      console.error(e)
    }
    socket.write(build(reqId, data, data))
    return true
  }
}

async function processAdmin(socket, reqId, commandName, doc) {
  switch (commandName) {
    case 'listdatabases':
      let res
      try {
        res = await doQuery('SELECT datname AS name FROM pg_database WHERE datistemplate = false')
      } catch (e) {
        console.error(e)
        res = { rows: [] }
      }
      const data = { 'databases': res.rows, 'ok': 1 }
      socket.write(wire.createCommandReply(reqId, data, data))
      break
    case 'whatsmyuri':
      socket.write(wire.createCommandReply(reqId, {}, { 'you': '127.0.0.1:56709', 'ok': 1 }))
      break
    case 'getlog':
      if (doc.getLog === 'startupWarnings') {
        const log = {
          'totalLinesWritten': 1,
          'log': ['This is a Postgres database using the Mongo wrapper.'],
          'ok': 1
        }
        socket.write(wire.createCommandReply(reqId, log, log))
      } else {
        return false
      }
      break
    case 'replsetgetstatus':
      socket.write(wire.createCommandReply(reqId, {}, adminReplies.replSetGetStatus()))
      break
    case 'serverstatus':
      socket.write(wire.createCommandReply(reqId, adminReplies.getServerStatus(), {}))
      break
    case 'currentop':
      const reply = {
        inprog: [],
        ok: 1
      }
      socket.write(wire.createCommandReply(reqId, reply, reply))
      break
    default:
      return false
  }
  return true
}

let previousData

async function processRecord(socket, data) {
  if (previousData) {
    data = Buffer.concat([previousData, data])
    previousData = null
  }
  const { msgLength, reqId, opCode } = wire.parseHeader(data)

  if (data.length < msgLength - 1) {
    previousData = data
    return console.log('partial data received')
  }

  try {
    await handleRecord(socket, data, opCode, reqId)
  } catch (e) {
    console.error(e)
    if (e.code) {
      return socket.write(wire.createCommandReply(reqId, e, {}))
    }
    return socket.write(wire.createCommandReply(reqId, { errmsg: e.message, ok: 0 }, { ok: 0 }))
  }
}
async function handleRecord(socket, data, opCode, reqId) {
  if (opCode === wire.OP_QUERY) {
    const { doc, collectionName } = wire.parseQuery(data)
    if (collectionName === 'admin.$cmd' && (doc.isMaster || doc.ismaster)) {
      return socket.write(wire.createResponse(reqId, adminReplies.getIsMasterReply()))
    }
    for (const command of commands) {
      if (doc[command] || doc[command.toLowerCase()]) {
        if (await crud(socket, reqId, '', command, doc, wire.createResponse))
          return
      }
    }
    console.dir({ err: 'UNHANDLED REQUEST OP_QUERY', collectionName, doc })
    return socket.write(wire.createCommandReply(reqId, { ok: 0 }, { ok: 0 }))
  } else if (opCode === wire.OP_COMMAND) {
    const { databaseName, commandName, doc } = wire.parseCommand(data)
    if (await crud(socket, reqId, databaseName, commandName, doc, wire.createCommandReply))
      return

    if (databaseName === 'admin' || databaseName === 'db') {
      if (await processAdmin(socket, reqId, commandName, doc))
        return
    }

    let res
    switch (commandName) {
      case 'ismaster':
        return socket.write(wire.createCommandReply(reqId, adminReplies.getIsMasterReply(), adminReplies.getIsMasterReply()))
      case 'buildinfo':
        return socket.write(wire.createCommandReply(reqId, adminReplies.getBuildInfo(), {}))
      case 'listcollections':
        res = await client.query('SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' AND table_type=\'BASE TABLE\';')
        const tables = res.rows.map((row) => ({ name: row.table_name, type: 'collection', options: {}, info: { readOnly: false } }))
        const data = util.createCursor(`${databaseName}.$cmd.listCollections`, tables)
        return socket.write(wire.createCommandReply(reqId, data, data))
      case 'ping':
        return socket.write(wire.createCommandReply(reqId, {}, { 'ok': 1 }))
      case 'getlasterror':
        return socket.write(wire.createCommandReply(reqId, lastResult, lastResult))
      case 'dropdatabase':
        res = await doQuery('select string_agg(\'drop table "\' || tablename || \'" cascade\', \'; \') from pg_tables where schemaname = \'public\'')
        const dropQuery = res.rows[0].string_agg
        await doQuery(dropQuery)
        return socket.write(wire.createCommandReply(reqId, {}, { 'ok': 1 }))
    }

    console.dir({ err: 'UNHANDLED REQUEST', commandName, databaseName, doc })
    return socket.write(wire.createCommandReply(reqId, { ok: 0 }, { ok: 0 }))
  }
}

const mongoPort = parseInt(args[2] || '27018')
server.listen(mongoPort, '127.0.0.1')
console.log(`Connecting to postgres at ${pgHost}:5432`)
console.log(`Serving Mongo on port ${mongoPort}`)
