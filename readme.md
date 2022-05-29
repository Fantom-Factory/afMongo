# Mongo v2.1.0
---

[![Written in: Fantom](http://img.shields.io/badge/written%20in-Fantom-lightgray.svg)](https://fantom-lang.org/)
[![pod: v2.1.0](http://img.shields.io/badge/pod-v2.1.0-yellow.svg)](http://eggbox.fantomfactory.org/pods/afMongo)
[![Licence: ISC](http://img.shields.io/badge/licence-ISC-blue.svg)](https://choosealicense.com/licenses/isc/)

## Overview

Mongo is a pure Fantom driver for [MongoDB](http://www.mongodb.org/).

Mongo driver features:

* Developed against the MongoDB Stable API v1 from MongoDB 5.2+ (compatible with MongoDB 3.6+)
* Support for multi-document, multi-collection transactions
* Support for retryable read and write operations
* Support for Replica Set connection URLs
* Pooled Connection Manager for multi-threaded use and automatic topology scanning and connection fail over
* Pluggable authentication with a default [SCRAM-SHA-1 over SASL](http://www.alienfactory.co.uk/articles/mongodb-scramsha1-over-sasl) implementation
* `zlib` wire compression


Mongo API features:

* Standard CRUD commands: `insert()`, `find()`, `update()`, `replace()`, and `delete()`
* Special commands: `count()`, `aggregate()`, `findAndUpdate()`, `findAndDelete()`
* Index support commands: `create()`, `ensure()` and `drop()`
* Large query cursor support
* Text indexes and text searching
* Simplified Query DSL syntax> 
    **ALIEN-AID:** See [Morphia](http://eggbox.fantomfactory.org/pods/afMorphia) for a complete Fantom to MongoDB object mapping library!




## <a name="Install"></a>Install

Install `Mongo` with the Fantom Pod Manager ( [FPM](http://eggbox.fantomfactory.org/pods/afFpm) ):

    C:\> fpm install afMongo

Or install `Mongo` with [fanr](https://fantom.org/doc/docFanr/Tool.html#install):

    C:\> fanr install -r http://eggbox.fantomfactory.org/fanr/ afMongo

To use in a [Fantom](https://fantom-lang.org/) project, add a dependency to `build.fan`:

    depends = ["sys 1.0", ..., "afMongo 2.1"]

## <a name="documentation"></a>Documentation

Full API & fandocs are available on the [Eggbox](http://eggbox.fantomfactory.org/pods/afMongo/) - the Fantom Pod Repository.

## <a name="quickStart"></a>Quick Start

1. Start up an instance of MongoDB:    C:\> mongod
    
    MongoDB starting
    db version v5.2.0
    waiting for connections on port 27017


2. Create a text file called `Example.fan`    using afBson::BsonIO
    using afMongo
    
    class Example {
    
        Void main() {
            mongoClient := MongoClient(`mongodb://localhost:27017`)
            collection  := mongoClient.db("friends").collection("birds")
    
            documentIn  := [
                "_id"   : ObjectId(),
                "name"  : "Emma",
                "score" : 9
            ]
            collection.insert(documentIn)
    
            emma        := collection.find.toList.first
    
            echo("Emma:")
            echo(BsonIO().print(emma, 20))
    
            mongoClient.shutdown
        }
    }


3. Run `Example.fan` as a Fantom script from the command line:    C:\> fan Example.fan
    
          Fantom-Factory
      _____ ___ ___ ___ ___
     |     | . |   | . | . |
     |_|_|_|___|_|_|_  |___|
                  |___|2.1.0
    
    Connected to MongoDB v5.2.0 (at mongodb://localhost:27017)
    
    Emma:
    {
      "_id"   : ObjectId("57fe499fa81320d933000001"),
      "name"  : "Emma"
      "score" : 9,
    }




## Usage

[MongoClient](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoClient) is the main entry point into `Mongo`, with [MongoConnMgr](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoConnMgr) underpinning everything. From there you can access all other components of MongoDB, namely [MongoDb](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoDb), [MongoColl](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoColl), [MongoIdx](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoIdx).

    MongoConnMgr                  : Connection Pool
      +-- MongoClient             : Client
           +-- MongoDb            : Database
                +-- MongoColl     : Collection
                     +-- MongoIdx : Index
    

## Connecting

`MongoClient` may be created with a [MongoConnMgr](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoConnMgr), which manages your connections to MongoDB for application wide, multi-threaded use:

    connMgr := MongoConnMgr(`mongodb://localhost:27017`)
    client  := MongoClient(connMgr)
    

When you create a `MongoClient` it immediately connects to MongoDB and verifies the version:

    .    Fantom-Factory
     _____ ___ ___ ___ ___
    |     | . |   | . | . |
    |_|_|_|___|_|_|_  |___|
                  |___|2.1.0
    
    Connected to MongoDB v5.2.0
    

Note that `MongoConnMgrPool` will always query the supplied MongoDB host(s) to find the primary node, on which all read and write operations are performed.

## Queries

Mongo works with BSON documents.

BSON documents are represented in Fantom as a Map of type `[Str:Obj?]`. All document keys must be strings. Document values can be any valid [BSON](http://eggbox.fantomfactory.org/pods/afBson) type.

MongoDB stores BSON documents in Collections. Use the `find()` methods to query a collection. Using the `friends` database in the [QuickStart Example](#quickStart) we could do:

    collection.findOne( ["name":"Emma"] )       // --> return the doc where 'name == Emma'
                                                //     ('Emma' must be unique)
    
    collection.find                             // --> return ALL docs in the collection
    collection.find( ["name":"Emma"] )          // --> return all docs where 'name == Emma'
    collection.find( ["score": ["\$gt":7]] )    // --> return all docs with 'score > 7'
    

The `$gt` expression is an example of a [Query operator](http://docs.mongodb.org/manual/reference/operator/query/).

This driver also supplies a little DSL for creating queries, in the form of [MongoQ](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoQ), which can make Mongo queries much easier to write.

    // query using standard BSON
    query := [
        "\$and" : [
            ["\$or": [["price": 0.99f], ["price": 1.99f]]],
            ["\$or": [["sale" : true ], ["qty"  : ["\$lt": 20]]]]
        ]
    ]
    
    // same query using afMongo's DSL
    query := MongoQ {
        and(
            or( eq("price", 0.99f), eq("price", 1.99f)  ),
            or( eq("sale", true),   lessThan("qty", 29) )
        )
    }.query
    
    collection.find(query)
    

To iterate over *large* result sets without loading it all into memory, use a [Cursor](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoCur). `Cursors` download documents in batches, behind the scenes, as and when required. Create and use a `Cursor` by using the `find()` method:

    cursor := coll.find( ["score": ["\$gt":2]] )
    cursor.batchSize = 10
    
    while (cursor.isAlive)
        doc := cursor.next
        ...
    }
    
    cursor.kill
    

## Write Commands

The `insert()` command is simple enough and is demonstrated in the [QuickStart example](#quickStart).

`update()`, `replace()`, and `delete()` are similar in that they both take a query that describes which document(s) are to be updated / deleted. For most usages this will a simply be the id of the document in question:

    collection.replace( ["_id": objId], [ ...new doc...] )
    

Mongo will throw a `MongoErr` should a write error occur.

## ObjectId

All documents held in a collection need a unique id, held in a field named `_id`. If the `_id` field does not exist, MongoDB will create one for you of type [ObjectId](http://eggbox.fantomfactory.org/pods/afBson/api/ObjectId).

Note that `_id` does not need to an `ObjectId`, it can be any BSON type. It just needs to be unique in the collection.

Like [marmite](http://www.ilovemarmite.com/), people tend to have a love / hate relationship with the `ObjectId`. The good comments revolve around it having a natural sort that (roughly) corresponds to creation time. The bad is that it's a large human-unfriendly 24 char identifier, and in *humongous* collections it eats up precious bytes which means the [index may not fit into RAM](http://docs.mongodb.org/manual/tutorial/ensure-indexes-fit-ram/).

If sequential integers are more your thing when it comes to IDs, then [MongoSeqs](http://eggbox.fantomfactory.org/pods/afMongo/api/MongoSeqs) contains helper methods that use atomic updates on a named Collection to keep track of generated IDs.

## Connection Fail Over

The `MongoConnMgrPool` accepts a replica set URL with multiple hosts (with optional ports):

    mongodb://db1.example.net,db2.example.net:2500/?connectTimeoutMS=30000

When  `startup()` is called, the hosts are queried to find the primary / master node. All read and write operations are then performed on this primary node.

When a connection to the master node is lost, all hosts are automatically re-queried to find a new master.

## Remarks

The Fantom-Factory MongoDB driver was inspired by [fantomongo](https://bitbucket.org/liamstask/fantomongo) by Liam Staskawicz.

If you're looking for cross-platform MongoDB GUI client then look no further than [Robomongo](http://robomongo.org/) / Robo 3T / Studio 3T Free!

