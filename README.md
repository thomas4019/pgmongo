# postres-mongo-emulator
This implements the MongoDB wire protocol and adapts queries to work with a PostgreSQL database using jsonb fields.
This project aims to be a full drop-in replacement of MongoDB. It's a work in progress, but is in a near usable state.
I've tested it with [Keystone.js](http://keystonejs.com/) and it seemed to work reasonably well.

# Getting Started
```bash
npm install -g pgmongo
pgmongo [pg_database_name] [pg_host] [mongo_port] // e.g. pgmongo mydatabase localhost 27018
```

# Current status
Currently passes 150 of the 916 core mongo [jstests](https://github.com/mongodb/mongo/tree/master/jstests/core).

## Missing Features (ordered by priority)
* ObjectIDs (other than _id)
* Preserve BSON
* Queries matching array elements
* Cursor support
* Indexes (add and remove)
* [min](https://docs.mongodb.com/manual/reference/method/cursor.min/) and max
* findandmodify
* Capped collections
* aggregation framework
* explain queries
* numeric keys (currently numbers are assumed to be an array index)

# Cannot support
* NaN and Infinity
* Non-homogeneous data

## Resources
* [MongoDB Wire Protocol](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/)
* [Admin Commands](https://docs.mongodb.com/manual/reference/command/nav-administration/)
* [JSONB Index Performance](http://bitnine.net/blog-postgresql/postgresql-internals-jsonb-type-and-its-indexes/)

## Related Projects
* [Mongres](https://github.com/umitanuki/mongres)
* [mongolike](https://github.com/JerrySievert/mongolike)
* [plv8](https://github.com/plv8/plv8)
* [ToroDB](https://news.ycombinator.com/item?id=8527013)