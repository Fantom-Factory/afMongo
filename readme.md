## Overview 

`Mongo` is a pure Fantom driver for [MongoDB](http://www.mongodb.org/) v2.6+.

`Mongo` driver features:

- Standard and capped collections
- Write commands: `insert()`, `update()`, `delete()` and `findAndModify()`
- Write concern support (v2.6+)
- Optimised queries for `findOne()` and `findAll()`
- Cursor support
- Aggregation commands: `aggregate()`, `distinct()`, `group()` and `mapReduce()`
- Index support: `create()`, `ensure()` and `drop()`
- User support: `create()`, `drop()`, `grant()` and `revoke()` roles
- Database authentication
- Server side `eval()` commands
- Pooled connection manager for multi-threaded use
- Support for Replica Set connection URLs

`Mongo` driver has been written specifically for MongoDB v2.6.0 or newer.

Many features, including ALL write commands, will **NOT** work with older MongoDB versions.

> **ALIEN-AID:** See [Morphia](http://www.fantomfactory.org/pods/afMorphia) for a complete Fantom to MongoDB object mapping library!

## Install 

Install `Mongo` with the Fantom Repository Manager ( [fanr](http://fantom.org/doc/docFanr/Tool.html#install) ):

    C:\> fanr install -r http://repo.status302.com/fanr/ afMongo

To use in a [Fantom](http://fantom.org/) project, add a dependency to `build.fan`:

    depends = ["sys 1.0", ..., "afMongo 1.0"]

## Documentation 

Full API & fandocs are available on the [Status302 repository](http://repo.status302.com/doc/afMongo/).

## Quick Start 

1). Start up an instance of MongoDB:

```
C:\> mongod

MongoDB starting
db version v2.6.0
waiting for connections on port 27017
```

2). Create a text file called `Example.fan`:

```
using afBson
using afMongo
using concurrent

class Example {

    Void main() {
        mongoClient := MongoClient(ActorPool(), `mongodb://localhost:27017`)
        collection  := mongoClient.db("friends").collection("birds")

        documentIn  := [
            "_id"   : ObjectId(),
            "name"  : "Emma",
            "score" : 9
        ]
        collection.insert(documentIn)

        echo( collection.findAll.first )

        mongoClient.shutdown
    }
}
```

3). Run `Example.fan` as a Fantom script from the command line:

```
C:\> fan Example.fan

     Alien-Factory
 _____ ___ ___ ___ ___
|     | . |   | . | . |
|_|_|_|___|_|_|_  |___|
              |___|1.0.0

Connected to MongoDB v2.6.0 (at mongodb://localhost:27017)

[_id:5373acbda8000b3491000001, name:Emma, score:9]
```

## Usage 

[MongoClient](http://repo.status302.com/doc/afMongo/MongoClient.html) is the main entry point into `Mongo`. From there you can access all other components of MongoDB, namely [Database](http://repo.status302.com/doc/afMongo/Database.html), [Collection](http://repo.status302.com/doc/afMongo/Collection.html), [Index](http://repo.status302.com/doc/afMongo/Index.html) and [User](http://repo.status302.com/doc/afMongo/User.html).

```
MongoClient
 `-- Database
      +-- Collection
      |    `-- Index
      `-- User
```

### Connecting 

`MongoClient` is created with a [ConnectionManager](http://repo.status302.com/doc/afMongo/ConnectionManager.html), which manages your connections to MongoDB. Use [ConnectionManagerPooled](http://repo.status302.com/doc/afMongo/ConnectionManagerPooled.html) for normal multi-threaded use:

    conMgr := ConnectionManagerPooled(ActorPool(), `mongodb://localhost:27017`)
    client := MongoClient(conMgr)

When you create a `MongoClient` it immediately connects to MongoDB and verifies it is the correct version:

```
.    Alien-Factory
 _____ ___ ___ ___ ___
|     | . |   | . | . |
|_|_|_|___|_|_|_  |___|
              |___|1.0.0

Connected to MongoDB v2.4.9

[warn] ****************************************************************************
[warn] ** WARNING: This driver is ONLY compatible with MongoDB v2.6.0 or greater **
[warn] ****************************************************************************
```

Ooops! As you can see, we have an old MongoDB running. And true enough, when we run the [QuickStart example](http://repo.status302.com/doc/afMongo/#quickStart.html) we get:

    afMongo::MongoCmdErr: Command 'insert' failed. MongoDB says: no such cmd: insert

Installing (and connection to) a fresh MongoDB of version 2.6.0 or greater will get you back on track.

Note that `ConnectionManagerPooled` will always query the supplied MongoDB host(s) to find the primary node, on which all read and write operations are performed.

### Queries 

`Mongo` and MongoDB work with documents, they are used throughout the `Mongo` API. A MongoDB document is represented in Fantom as a Map of type `[Str:Obj?]`. All document keys must be strings. Document values can be any valid [BSON](http://www.fantomfactory.org/pods/afBson) type.

A MongoDB database stores documents in collections. Use the `find()` methods to query a collection. Using the `friends` database in the [QuickStart Example](http://repo.status302.com/doc/afMongo/#quickStart.html) we could do:

```
collection.findOne( ["name":"Emma"] )       // --> return the doc where 'name == Emma'
                                            //     ('Emma' must be unique)

collection.findAll                          // --> return ALL docs in the collection
collection.findAll( ["name":"Emma"] )       // --> return all docs where 'name == Emma'
collection.findAll( ["score": ["\$gt":7]] ) // --> return all docs with 'score > 7'
```

The `$gt` expression is an example of a [Query operator](http://docs.mongodb.org/manual/reference/operator/query/).

To iterate over a *massive* collection without loading it all into memory, use a [Cursor](http://repo.status302.com/doc/afMongo/Cursor.html). `Cursors` download documents in batches, behind the scenes, as and when required. Create and use a `Cursors` by using the `find()` method:

```
collection.find( ["score": ["\$gt":2]] ) |cursor| {
    cursor.batchSize = 10

    while (cursor.hasNext)
        doc := cursor.next
        ...
    }
}
```

### Write Commands 

The `insert()` command is simple enough and is demonstrated in the [QuickStart example](http://repo.status302.com/doc/afMongo/#quickStart.html).

`update()` and `delete()` are similar in that they both take a query that describes which document(s) are to be updated / deleted. For most usages this will a simply be the id of the document in question:

    collection.update( ["_id": objId], [ ...new doc...] )

Note that as of MongoDB v2.6 there is longer any need to call a `getLastError()` function. All error handling is done via write concerns. By default `Mongo` will throw a `MongoErr` should a write error occur.

### ObjectId 

All documents held in a collection need a unique id, held in a field named `_id`. If the `_id` field does not exist, MongoDB will create one for you of type [ObjectId](http://repo.status302.com/doc/afBson/ObjectId.html).

Note that `_id` does not need to an `ObjectId`, it can be any BSON type. It just needs to be unique in the collection.

Like [marmite](http://www.ilovemarmite.com/), people tend to have a love / hate relationship with the `ObjectId`. The good comments revolve around it having a natural sort that (roughly) corresponds to creation time. The bad is that in *humongous* collections it eats up precious bytes which means the [index can't fit into RAM](http://docs.mongodb.org/manual/tutorial/ensure-indexes-fit-ram/).

## Remarks 

The Alien-Factory MongoDB driver was inspired by [fantomongo](https://bitbucket.org/liamstask/fantomongo) by Liam Staskawicz.

If you're looking for cross-platform MongoDB GUI client then look no further than [Robomongo](http://robomongo.org/)!

