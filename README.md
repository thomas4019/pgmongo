# postres-mongo-emulator
Node.js program that implements the Mongo wire protocol and adapts queries to use Postgres tables with a single jsonb field called "data".
This project aims to be a full drop-in replacement of MongoDB. It's still very much a work in progress, but is in a near usable state.

# Getting Started
```bash
npm install -g pgmongo
pgmongo [pg_database_name] [pg_host] [mongo_port] // e.g. pgmongo mydatabase localhost 27018
```

## Missing Features
* Handling ObjectIDs
* BSON Features
** Real floats including NaN and Infinity
* Cursor support
* Capped collections
* Queries matching array elements
* Remove indexes - try jstests/core/in5.js
* findandmodify

## Resources
* [MongoDB Wire Protocol](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/)
* [Admin Commands](https://docs.mongodb.com/manual/reference/command/nav-administration/)
* [JSONB Index Performance](http://bitnine.net/blog-postgresql/postgresql-internals-jsonb-type-and-its-indexes/)

## Related Projects
* [Mongres](https://github.com/umitanuki/mongres)
* [mongolike](https://github.com/JerrySievert/mongolike)
* [plv8](https://github.com/plv8/plv8)
* [ToroDB](https://news.ycombinator.com/item?id=8527013)