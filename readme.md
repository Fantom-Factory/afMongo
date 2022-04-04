# Mongo v1.2.0
---

[![Written in: Fantom](http://img.shields.io/badge/written%20in-Fantom-lightgray.svg)](https://fantom-lang.org/)
[![pod: v1.2.0](http://img.shields.io/badge/pod-v1.2.0-yellow.svg)](http://eggbox.fantomfactory.org/pods/afMongo)
[![Licence: ISC](http://img.shields.io/badge/licence-ISC-blue.svg)](https://choosealicense.com/licenses/isc/)

## Overview

Mongo is a pure Fantom driver for [MongoDB v3.2+](http://www.mongodb.org/).

Mongo driver features:

* Compatible with MongoDB v3.2+
* Standard and capped collections
* Pooled connection manager for multi-threaded use and automatic connection fail over.
* Write commands: `insert()`, `update()`, `delete()` and `findAndModify()`
* Optimised queries for `findOne()` and `findAll()`
* Aggregation commands: `aggregate()`, `distinct()`, `group()` and `mapReduce()`
* Index support: `create()`, `ensure()` and `drop()`
* User support: `create()`, `drop()`, `grant()` and `revoke()` roles
* Server side `eval()` commands
* Database authentication
* Cursor support
* Write concern support
* Text indexes and text searching
* Support for Replica Set connection URLs


Mongo driver has been written specifically for MongoDB v3.2 or newer.

> **ALIEN-AID:** See [Morphia](http://eggbox.fantomfactory.org/pods/afMorphia) for a complete Fantom to MongoDB object mapping library!


## <a name="Install"></a>Install

Install `Mongo` with the Fantom Pod Manager ( [FPM](http://eggbox.fantomfactory.org/pods/afFpm) ):

    C:\> fpm install afMongo

Or install `Mongo` with [fanr](https://fantom.org/doc/docFanr/Tool.html#install):

    C:\> fanr install -r http://eggbox.fantomfactory.org/fanr/ afMongo

To use in a [Fantom](https://fantom-lang.org/) project, add a dependency to `build.fan`:

    depends = ["sys 1.0", ..., "afMongo 1.2"]

## <a name="documentation"></a>Documentation

Full API & fandocs are available on the [Eggbox](http://eggbox.fantomfactory.org/pods/afMongo/) - the Fantom Pod Repository.

## <a name="quickStart"></a>Quick Start

1. Start up an instance of MongoDB:    C:\> mongod
    
    MongoDB starting
    db version v3.2.10
    waiting for connections on port 27017


2. Create a text file called `Example.fan`    using afBson
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
    
            emma   := collection.findAll.first
            result := PrettyPrinter { it.maxWidth = 20 }.print(emma)
    
            echo("Emma:")
            echo(result)
    
            mongoClient.shutdown
        }
    }


3. Run `Example.fan` as a Fantom script from the command line:    C:\> fan Example.fan
    
          Alien-Factory
      _____ ___ ___ ___ ___
     |     | . |   | . | . |
     |_|_|_|___|_|_|_  |___|
                  |___|1.1.0
    
    Connected to MongoDB v3.2.10 (at mongodb://localhost:27017)
    
    Emma:
    {
      "_id"   : ObjectId("57fe499fa81320d933000001"),
      "name"  : "Emma"
      "score" : 9,
    }




## Usage

[MongoClient](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoClient) is the main entry point into `Mongo`. From there you can access all other components of MongoDB, namely [Database](http://eggbox.fantomfactory.org/pods/afMongo/api/Database), [Collection](http://eggbox.fantomfactory.org/pods/afMongo/api/Collection), [Index](http://eggbox.fantomfactory.org/pods/afMongo/api/Index) and [User](http://eggbox.fantomfactory.org/pods/afMongo/api/User).

    MongoClient
     `-- Database
          +-- Collection
          |    `-- Index
          `-- User
    

## Connecting

`MongoClient` is created with a [ConnectionManager](http://eggbox.fantomfactory.org/pods/afMongo/api/ConnectionManager), which manages your connections to MongoDB. Use [ConnectionManagerPooled](http://eggbox.fantomfactory.org/pods/afMongo/api/ConnectionManagerPooled) for normal multi-threaded use:

    conMgr := ConnectionManagerPooled(ActorPool(), `mongodb://localhost:27017`)
    client := MongoClient(conMgr)

When you create a `MongoClient` it immediately connects to MongoDB and verifies the version:

    .    Alien-Factory
     _____ ___ ___ ___ ___
    |     | . |   | . | . |
    |_|_|_|___|_|_|_  |___|
                  |___|1.1.0
    
    Connected to MongoDB v3.2.10
    

Note that `ConnectionManagerPooled` will always query the supplied MongoDB host(s) to find the primary node, on which all read and write operations are performed.

## Queries

`Mongo` and MongoDB work with documents, they are used throughout the `Mongo` API. A MongoDB document is represented in Fantom as a Map of type `[Str:Obj?]`. All document keys must be strings. Document values can be any valid [BSON](http://eggbox.fantomfactory.org/pods/afBson) type.

A MongoDB database stores documents in collections. Use the `find()` methods to query a collection. Using the `friends` database in the [QuickStart Example](#quickStart) we could do:

    collection.findOne( ["name":"Emma"] )       // --> return the doc where 'name == Emma'
                                                //     ('Emma' must be unique)
    
    collection.findAll                          // --> return ALL docs in the collection
    collection.findAll( ["name":"Emma"] )       // --> return all docs where 'name == Emma'
    collection.findAll( ["score": ["\$gt":7]] ) // --> return all docs with 'score > 7'
    

The `$gt` expression is an example of a [Query operator](http://docs.mongodb.org/manual/reference/operator/query/).

To iterate over a *massive* collection without loading it all into memory, use a [Cursor](http://eggbox.fantomfactory.org/pods/afMongo/api/Cursor). `Cursors` download documents in batches, behind the scenes, as and when required. Create and use a `Cursors` by using the `find()` method:

    collection.find( ["score": ["\$gt":2]] ) |cursor| {
        cursor.batchSize = 10
    
        while (cursor.hasNext)
            doc := cursor.next
            ...
        }
    }
    

## Write Commands

The `insert()` command is simple enough and is demonstrated in the [QuickStart example](#quickStart).

`update()` and `delete()` are similar in that they both take a query that describes which document(s) are to be updated / deleted. For most usages this will a simply be the id of the document in question:

    collection.update( ["_id": objId], [ ...new doc...] )

Note that as of MongoDB v2.6 there is longer any need to call a `getLastError()` function. All error handling is done via write concerns. By default `Mongo` will throw a `MongoErr` should a write error occur.

## ObjectId

All documents held in a collection need a unique id, held in a field named `_id`. If the `_id` field does not exist, MongoDB will create one for you of type [ObjectId](http://eggbox.fantomfactory.org/pods/afBson/api/ObjectId).

Note that `_id` does not need to an `ObjectId`, it can be any BSON type. It just needs to be unique in the collection.

Like [marmite](http://www.ilovemarmite.com/), people tend to have a love / hate relationship with the `ObjectId`. The good comments revolve around it having a natural sort that (roughly) corresponds to creation time. The bad is that it's a large human-unfriendly 24 char identifier, and in *humongous* collections it eats up precious bytes which means the [index may not fit into RAM](http://docs.mongodb.org/manual/tutorial/ensure-indexes-fit-ram/).

## Authentication

To set a default user to be used by all connections, set the username and password in the MongoDB connection URL:

    conMgr := ConnectionManagerPooled(ActorPool(), `mongodb://<username>:<password>@localhost:27017`)
    client := MongoClient(conMgr)

Another way, that may also be used in conjunction with URL credentials, is to use an authenticated connection. Authenticated connections are bound to the database they are authenticated against.

    client := MongoClient(conMgr)
    db     := client["database"]
    data   := db.authenticate("ZeroCool", "password") |authDb -> Obj?| {
        ...
        return authDb["top-secret"].findAll
    }
    

All Mongo objects ( `Collection`, `Index`, `User`, etc...) created from the authenticated database will inherit the user credentials. Note that the database *must* be accessed via the `authDb` variable for the commands to be authenticated.

Note that authentication defaults to `SCRAM-SHA-1` but basic `MONGODB-CR` is also supported.

## Connection Fail Over

The `PooledConnectionManager` accepts a replica set URL with multiple hosts (with optional ports):

    mongodb://db1.example.net,db2.example.net:2500/?connectTimeoutMS=30000

When  `startup()` is called, the hosts are queried to find the primary / master node. All read and write operations are then performed on this primary node.

When a connection to the master node is lost, all hosts are automatically re-queried to find a new master.

## Remarks

The Alien-Factory MongoDB driver was inspired by [fantomongo](https://bitbucket.org/liamstask/fantomongo) by Liam Staskawicz.

If you're looking for cross-platform MongoDB GUI client then look no further than [Robomongo](http://robomongo.org/)!

