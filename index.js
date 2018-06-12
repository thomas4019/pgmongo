const net = require('net')
const BSON = require('bson-ext')
const mongoToPostgres = require('mongo-query-to-postgres-jsonb')
const debug = require('debug')
const debugQuery = debug('pgmongo:pgquery')
const bson = new BSON([BSON.Binary, BSON.Code, BSON.DBRef, BSON.Decimal128, BSON.Double, BSON.Int32, BSON.Long, BSON.Map, BSON.MaxKey, BSON.MinKey, BSON.ObjectId, BSON.BSONRegExp, BSON.Symbol, BSON.Timestamp])
const Long = BSON.Long

const OP_REPLY = 1
const OP_QUERY = 2004
const OP_COMMAND = 2010
const OP_COMMANDREPLY = 2011

const adminReplies = require('./admin')
const args = process.argv.slice(2);

if (args.length < 1) {
  console.error('node index.js <database> [<pghost>] [<mongoport>]');
  return;
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

function listIndicesQuery(fieldName, collectionName) {
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

async function doQuery(pgQuery) {
  debugQuery(pgQuery)
  const res = await client.query(pgQuery)
  return res
}

async function createTable(collectionName) {
  const query = `CREATE TABLE IF NOT EXISTS ${collectionName} (data jsonb)`
  const res = await doQuery(query)
}

async function tryOrCreateTable(action, collectionName) {
  try {
    return await action();
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
  socket.on('data', (data) => processRecord(socket, data));
})

async function crud(socket, reqId, databaseName, commandName, doc) {
  let collectionName = '"' + doc[commandName] + '"'
  if (commandName === 'find') {
    const filter = doc.filter
    let where = ''
    let select = ''
    try {
      where = mongoToPostgres('data', filter)
      select = mongoToPostgres.convertSelect('data', doc.projection)
    }
    catch (err) {
      const data = {
        errmsg: 'in must be an array',
        ok: 0
      }
      socket.write(createCommandReply(reqId, data, {}))
      return true
    }
    let query = `SELECT ${select} FROM ${collectionName} WHERE ${where}`;
    if (doc.sort) {
      if (doc.collation && doc.collation.numericOrdering) {
        query += ' ORDER BY cast(' + mongoToPostgres.convertSort('data', doc.sort) + ' as double precision)'
      } else {
        query += ' ORDER BY ' + mongoToPostgres.convertSort('data', doc.sort)
      }
    }
    if (doc.limit) {
      query += ' LIMIT ' + Math.abs(doc.limit);
    }
    if (doc.skip) {
      if (doc.skip < 0) {
        socket.write(createCommandReply(reqId, { ok: 0, errmsg: 'negative skip not allowed' }, {}))
        return true
      }
      query += ' OFFSET ' + doc.skip;
    }
    let res;
    try {
      res = await doQuery(query)
    } catch (e) {
      console.error(e)
      res = { rows: [] }
    }
    const rows = res.rows.map((row) => row.data)
    debug('pgmongo:rows')(rows)
    const data = {
      'cursor': {
        'id': Long.fromNumber(0),
        'ns': databaseName + '.' + collectionName,
        'firstBatch': rows
      },
      'ok': 1
    }
    socket.write(createCommandReply(reqId, data, {}))
    return true
  }
  if (commandName === 'count') {
    const filter = doc.query
    const where = mongoToPostgres('data', filter)
    let query = `SELECT COUNT(*) FROM ${collectionName} WHERE ${where}`;
    const data = {
      n: 0,
      ok: 1
    }
    try {
      const res = await doQuery(query)
      data.n = res.rows[0].count
      if (doc.skip) {
        if (doc.skip < 0) {
          socket.write(createCommandReply(reqId, { ok: 0, errmsg: 'negative skip not allowed' }, {}))
          return true
        }
        data.n = Math.max(0, data.n - doc.skip)
      }
      if (doc.limit) {
        data.n = Math.min(Math.abs(doc.limit), data.n)
      }
    } catch (e) {
      console.error(e)
    }
    socket.write(createCommandReply(reqId, data, data))
    return true
  }
  if (commandName === 'update') {
    const update = doc.updates[0];
    const where = mongoToPostgres('data', update.q)
    let res
    try {
      const newValue = mongoToPostgres.convertUpdate('data', update.u)
      let query = `UPDATE ${collectionName} SET data = ${newValue} WHERE ${where}`
      // TODO (handle multi)
      res = await doQuery(query)
      lastResult.n = res.rowCount
      socket.write(createCommandReply(reqId, lastResult, lastResult))
      return true
    } catch (e) {
      console.error(e)
      if (e.message.includes('_id')) {
        const data = {
          errmsg: e.message,
          ok: 0
        }
        socket.write(createCommandReply(reqId, data, data))
        return true
      }
      // Can also upsert, fallback
      if (typeof update.u._id !== 'undefined' || update.upsert) {
        console.error('failed to update, falling back to insert');
        commandName = 'insert'
        doc.documents = [update.u]
      }
    }
  }
  if (commandName === 'insert') {
    const value = doc.documents[0];
    const newValue = mongoToPostgres.convertUpdate('data', value)
    async function insert() {
      let query = `INSERT INTO ${collectionName} VALUES (${newValue})`
      const res = await doQuery(query)
      lastResult.n = res.rowCount
      socket.write(createCommandReply(reqId, lastResult, lastResult))
    }

    tryOrCreateTable(insert, collectionName)
    return true
  }
  if (commandName === 'delete') {
    async function del() {
      const del = doc.deletes[0];
      const where = mongoToPostgres('data', del.q)
      let query = `DELETE FROM ${collectionName} WHERE ${where}`
      // TODO (handle multi)
      const res = await doQuery(query)
      lastResult.n = res.rowCount
      return socket.write(createCommandReply(reqId, lastResult, lastResult))
    }
    tryOrCreateTable(del, collectionName)
    return true
  }
  if (commandName === 'drop') {
    const del = doc.drop[0]
    const query = `DROP TABLE ${collectionName}`
    try {
      const res = await doQuery(query)
    } catch (e) {
      // often not an error if already doesn't exist
    }
    socket.write(createCommandReply(reqId, lastResult, lastResult))
    return true
  }
  if (commandName === 'validate') {
    let countQuery = `SELECT COUNT(*) FROM ${collectionName}`;
    const countRes = await client.query(countQuery)
    const data = {
      "ns" : databaseName + '.' + collectionName,
      "nrecords" : countRes.rows[0].count,
      "nIndexes" : 0,
      "keysPerIndex" : {},
      "valid" : true,
      "warnings" : [
        "Some checks omitted for speed. use {full:true} option to do more thorough scan."
      ],
      "errors" : [ ],
      "ok" : 1
    }
    return socket.write(createCommandReply(reqId, data, data))
  }
  if (commandName === 'listindexes') {
    const listIQuery = listIndicesQuery('data', doc[commandName])
    const indexRes = await doQuery(listIQuery)
    console.log(listIQuery)
    console.dir(indexRes)
    const indices = indexRes.rows.map((row) => ({
      v: 2,
      key: { _id: 1 },
      name: row.index_name,
      ns: databaseName + '.' + collectionName
    }))
    const data = {
      cursor: {
        id: Long.fromNumber(0),
        ns: databaseName + '.' + collectionName,
        firstBatch: indices
      },
      ok: 1
    }
    socket.write(createCommandReply(reqId, data, data))
    return true
  }
  if (commandName === 'createindexes') {
    const data = {
      createIndexes: {
        numIndexesBefore: 0,
        numIndexesAfter: 0,
        ok: 1
      },
      ok: 1
    }
    const listIQuery = listIndicesQuery('data', doc.createIndexes)
    const indexRes = await doQuery(listIQuery)
    const indices = indexRes.rows.map((row) => row.index_name)
    data.numIndexesBefore = indices.length
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
        const res = await doQuery(indexQuery)
      } catch (e) {
        console.error('failed to create index')
        //console.error(e)
      }
      data.createIndexes.numIndexesAfter++;
    }
    socket.write(createCommandReply(reqId, data, data))
    return true
  }
  if (commandName === 'renameCollection') {
    const data = { ok: 1 }
    collectionName = doc[commandName].split('.', 2)[1]
    const newName = doc.to.split('.', 2)[1]
    let query = `ALTER TABLE ${collectionName} RENAME TO "${newName}"`
    if (doc.dropTarget) {
      query = `DROP TABLE IF EXISTS ${newName}; ` + query
    }
    try {
      const res = await doQuery(query)
    } catch (e) {
      data.ok = 0
      console.error(e)
    }
    socket.write(createCommandReply(reqId, data, data))
    return true
  }
}

async function processAdmin(socket, reqId, commandName, doc) {
  switch (commandName) {
    case 'listDatabases':
      const data = { 'databases': [{ 'name': 'admin' }, { 'name': 'racepass' }], 'ok': 1 }
      socket.write(createCommandReply(reqId, data, data))
      break
    case 'whatsmyuri':
      socket.write(createCommandReply(reqId, {}, { 'you': '127.0.0.1:56709', 'ok': 1 }))
      break
    case 'getLog':
      if (doc.getLog === 'startupWarnings') {
        const log = {
          'totalLinesWritten': 1,
          'log': ['This is a Postgres database using the Mongo wrapper.'],
          'ok': 1
        }
        socket.write(createCommandReply(reqId, log, log))
      } else {
        return false
      }
      break
    case 'replSetGetStatus':
      socket.write(createCommandReply(reqId, {}, adminReplies.replSetGetStatus()))
      break
    case 'serverStatus':
      socket.write(createCommandReply(reqId, adminReplies.getServerStatus(), {}))
      break
    case 'currentOp':
      const reply = {
        inprog: [],
        ok: 1
      }
      socket.write(createCommandReply(reqId, reply, reply))
      break
    default:
      return false
  }
  return true
}

async function processRecord(socket, data) {
  const msgLength = data.readInt32LE(0)
  const reqId = data.readInt32LE(4)
  const opCode = data.readInt32LE(12)
  debug('pgmongo:opcode')(opCode)

  if (opCode === OP_QUERY) {
    const flags = data.readInt32LE(16)
    const nameEnd = data.indexOf('\0', 20) + 1
    const collectionName = data.toString('utf8', 20, nameEnd - 1)
    const documents = []
    bson.deserializeStream(data, nameEnd + 8, 1, documents, 0)
    const doc = documents[0]
    if (collectionName === 'admin.$cmd' && doc.isMaster) {
      return socket.write(createResponse(reqId, adminReplies.getIsMasterReply()))
    }
  } else if (opCode === OP_COMMAND) {
    const databaseEnd = data.indexOf('\0', 16) + 1
    const databaseName = data.toString('utf8', 16, databaseEnd - 1)
    const commandEnd = data.indexOf('\0', databaseEnd) + 1
    const commandName = data.toString('utf8', databaseEnd, commandEnd - 1).toLowerCase()
    const documents = []
    bson.deserializeStream(data, commandEnd, 2, documents, 0)
    const doc = documents[0]

    try {
      if (await crud(socket, reqId, databaseName, commandName, doc))
        return
    } catch (e) {
      console.error(e)
    }

    if (databaseName === 'admin' || databaseName === 'db') {
      if (await processAdmin(socket, reqId, commandName, doc))
        return
    }

    if (commandName === 'ismaster'/* && doc.isMaster*/) {
      return socket.write(createCommandReply(reqId, adminReplies.getIsMasterReply(), adminReplies.getIsMasterReply()))
    }
    if (commandName === 'buildinfo') {
      return socket.write(createCommandReply(reqId, adminReplies.getBuildInfo(), {}))
    }
    if (commandName === 'listcollections') {
      const res = await client.query('SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' AND table_type=\'BASE TABLE\';')
      const tables = res.rows.map((row) => ({ name: row.table_name, type: 'collection', options: {}, info: { readOnly: false } }))
      const data = {
        'cursor': {
          'id': Long.fromNumber(0),
          'ns': 'racepass.$cmd.listCollections',
          'firstBatch': tables
        },
        'ok': 1
      }
      return socket.write(createCommandReply(reqId, data, data))
    }
    if (commandName === 'ping') {
      return socket.write(createCommandReply(reqId, {}, { 'ok': 1 }))
    }
    if (commandName === 'getlasterror') {
      return socket.write(createCommandReply(reqId, lastResult, lastResult))
    }

    console.error('UNHANDLED REQUEST')
    console.dir({
      commandName,
      databaseName,
      documents
    })
    return socket.write(createCommandReply(reqId, { ok: 0 }, { ok: 0 }))
  }
}

const mongoPort = parseInt(args[2] || '27018')
server.listen(mongoPort, '127.0.0.1')
console.log(`connecting to postgres at ${pgHost}:5432`)
console.log(`serving mongo on port ${mongoPort}`)

function getRandomInt(max) {
  max = max || Math.pow(2, 31) - 1
  return Math.floor(Math.random() * Math.floor(max))
}

function createCommandReply(reqId, metadata, commandReply) {
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

function createResponse(reqId, doc) {
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
