////////////////////////////////////////////////////////////////////////////////
//
//  Copyright 2010 Liam Staskawicz
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

using afBson

**
**  Collection
**
@NoDoc
const class CollectionO
{
  internal const Str name
  const DB db
  
  new make(DB db, Str name)
  {
    this.name = validateName(name)
    this.db = db
  }
  
  private Str validateName(Str name)
  {
    // if(name.containsChar('$') && !Regex("test").matches(name))
    //   throw Err("invalid collection name - can't contain \$")
    
    if(name.isEmpty)
      throw ArgErr("invalid collection name - can't be empty")
    
    if(name.startsWith(".") || name.endsWith("."))
      throw ArgErr("invalid collection name - ${name} can't start or end with .")

    return name
  }
  
  Map validate()
  {
    return db.command(["validate":this.name])
  }
  
  **
  ** Returns the full name of this Collection.
  ** Takes the form of 'dbname.collname'
  **
  Str fullName()
  {
    return "${db.name}.${name}"
  }
  
  **
  ** Return distinct values for a key in this Collection.
  ** 'query' narrows the range of objects to those that match.
  **
  List distinct(Str key, Str:Obj? query := [:])
  {
    cmd := [:] { ordered = true }
    cmd.set("distinct", name).set("key", key).set("query", query)
    res := db.command(cmd)
    if(!DB.cmdOk(res)) throw MongoOpErr("distinct failed - ${res}")
    return res["values"]
  }
  
  **
  ** Perform a map reduce operation - 'map' and 'reduce' are JavaScript functions.
  ** 'out' (optional) specifies the collection to store the results in. 'query'
  ** (optional) specifies a query to narrow the range of objects this is applied
  ** to - null will use all objects.  
  **
  ** Returns a Map in which the key "result" is the Collection name created for the reults.
  ** Other info - processing time, etc is also included
  **
  ** 'opts' can include the following:
  ** pre>
  ** Key         Value Type       Description
  ** ---         ----------       -----------
  ** query       Str:Obj?         A query to limit the objects involved
  ** sort        [Str:direction]  Str is field name, 'direction' is either Mongo.ASCENDING or Mongo.DESCENDING 
  ** finalize    Str              A javascript function to apply to the result set after the map/reduce operation has finished.
  ** out         Str              The name of the output collection. If specified, the collection will not be treated as temporary.
  ** keeptemp    Bool             If true, the generated collection will be persisted.
  ** verbose     Bool             if true, provides statistics on job execution time.
  ** <pre
  **
  ** See http://www.mongodb.org/display/DOCS/MapReduce
  **
  Str:Obj? mapReduce(Str map, Str reduce, Str out := "reduceOut", Str:Obj opts := [:])
  {
    mrcmd := [:] { ordered = true }
    mrcmd.set("mapreduce", name).set("map", map).set("reduce", reduce).set("out", out)
    mrcmd.addAll(opts) // should probably make sure this is merged so above is not overwritten...
    res := db.command(mrcmd)
    if(!DB.cmdOk(res)) throw MongoOpErr("mapReduce failed - ${res}")
    return res
  }
  
  **
  ** 'key' - Str is key name, 
  **
  ** See http://www.mongodb.org/display/DOCS/Aggregation
  **
  ** TODO - support $keyf option
  **
  [Str:Obj?][] group(Str[] keys, Str:Obj? initial, Code reduce, Str:Obj? query := [:], Str? finalize := null)
  {
    Str:Bool keymap := [:]
    keys.each |s| { keymap[s] = true }
    args := ["ns": name, "key": keymap, "cond": query, "\$reduce": reduce, "initial": initial]
    if (finalize != null) args["finalize"] = finalize
    
    res := db.command(["group": args])
    if (!DB.cmdOk(res)) throw MongoOpErr("group failed - ${res}")
    return res["retval"]
  }
  
  **
  ** See `DB.indexInfo` for details.
  **
  Str:Obj indexInfo()
  {
    return db.indexInfo(this.name)
  }
  
  **
  **  See `DB.indexExists` for details.
  **
  Bool indexExists(Str indexName)
  {
     return db.indexExists(this.name, indexName)
  }
  
  **
  **  Ensures that an index exists in this collection, if it does not it creates it.
  **
  Str ensureIndex([Str:Int][] fields, Bool unique := false, Bool dropDups := false)
  {
    namestr := indexName(fields)
    if(indexExists(namestr))
    {
       return namestr
    }
       return createIndex(fields,unique, dropDups)
  }

  **
  **  Create a new index in this collection.
  **
  Str createIndex([Str:Int][] fields, Bool unique := false, Bool dropDups := false)
  {
    key := [:] { ordered = true }
    fields.each |map| { key.addAll(map) }
    namestr := indexName(fields)
    selector := ["name"    : namestr,
                  "ns"     : fullName,
                  "key"    : key,
                  "unique" : unique,
                  "dropDups" : dropDups]

    CollectionO(db, "system.indexes").insertDocs([selector], true) // add this in safe mode?
    return namestr
  }

  **
  ** Creates the index name used for the given fields.
  ** This is the same name that is returned from `Collection.createIndex`
  ** and is required by `Collection.dropIndex`
  **
  static Str indexName([Str:Int][] fields)
  {
    idxs := Str[,]
    fields.each |map, i| {
      k := map.keys.first
      idxs.add("${k}_${map[k]}")
    }
    return idxs.join("_")
  }
  
  **
  ** Drop this entire collection, including indexes.
  **
  Void drop()
  {
    db.dropCollection(this.name)
  }
  
  **
  ** Remove an index from a Collection.
  ** The required name can be obtained as the return value of `Collection.createIndex`
  ** of `Collection.indexName`
  **
  Void dropIndex(Str idxName)
  {
    db.dropIndex(this.name, idxName)
  }
  
  **
  ** Remove all indexes on this Collection
  **
  Void dropAllIndexes()
  {
    db.dropIndex(this.name, "*")
  }
  
  **
  ** Retrieve objects matching the given 'query', using the 'opts' provided.
  ** Note that this does not result in any communication with the DB - the 
  ** returned Cursor will fetch the results as needed.
  **
  ** 'opts' can include the following:
  ** pre>
  ** Key         Value Type    Description
  ** ---         ----------    -----------
  ** limit       Int           The maximum number of results to return
  ** skip        Int           Skip this number of documents in the result set
  ** batchsize   Int           The max number of documents to return in any intermediate fetch while iterating the Cursor
  ** fields      Str[]         A List of the field names to return, such that the entire object is not retrieved
  ** <pre
  **
  CursorO find(Str:Obj? query := [:], Str:Obj opts := [:])
  {
    return CursorO(this, query, opts)
  }
  
  **
  ** Find the first match only for the given query.
  ** See `Collection.find` for a description of relevant opts
  **
  Map? findOne(Str:Obj? query := [:], Str:Obj opts := [:])
  {
    opts["batchsize"] = -1 // only return one instance and close the cursor immediately
    return find(query, opts).next
  }
  
  **
  ** Convenience - if 'object' has previously been saved, 
  ** an `Collection.update` is performed, otherwise `Collection.insert`
  **
  Void save(Str:Obj? object, Bool safe := false)
  {
    if(object.containsKey("_id"))
      update(["_id":object["_id"]], object, true, safe)
    else
      insert(object, safe)
  }
  
  **
  ** Insert a document to the DB.
  ** Returns the inserted document.  To get the result of
  ** the insert operation, call `DB.lastErr` or set safe to true.
  ** In the latter case, the lack of a thrown `MongoOpErr` represents 
  ** a successful insert.
  **
  Void insert(Str:Obj? object, Bool safe := false)
  {
    insertDocs([object], safe)
  }
  
  **
  ** Insert a List of documents to the DB.
  ** Returns the objects inserted.  To get the result of
  ** the insert operation, call `DB.lastErr` or set safe to true.
  ** In the latter case, the lack of a thrown `MongoOpErr` represents 
  ** a successful insert.
  **
  Void insertDocs([Str:Obj?][] objects, Bool safe := false)
  {
    b := Buf() { endian = Endian.little }
    b.writeI4(0)                        // reserved
	BsonWriter(b.out).writeCString(fullName)// full name
    objects.each |obj| {                // bson objects
      // add _id to object if needed
      if(!obj.containsKey("_id"))
        obj["_id"] = ObjectId()
      BsonWriter(b.out).writeDocument(obj)
    }
    
    s := db.connection.getSocket()
    db.connection.sendMsg(s.out, b.flip, Mongo.OP_INSERT)
    // todo - read last error in strict mode
  }
  
  Map update(Str:Obj? query, Str:Obj? doc, Bool upsert := false, Bool multi := false, Bool safe := false)
  {
    b := Buf() { endian = Endian.little }
    b.writeI4(0) // reserved
    BsonWriter(b.out).writeCString(fullName) // full name
    opts := (upsert == true) ? 1 : 0
    if(multi == true)
      opts = opts.or(1.shiftl(1))
    b.writeI4(opts)
    BsonWriter(b.out).writeDocument(query)
    BsonWriter(b.out).writeDocument(doc)
    
    s := db.connection.getSocket()
    db.connection.sendMsg(s.out, b.flip, Mongo.OP_UPDATE)
    // todo - read last error in strict mode
    return doc
  }
  
  Void remove(Str:Obj? query := [:], Bool safe := false)
  {
    b := Buf() { endian = Endian.little }
    b.writeI4(0) // reserved
    BsonWriter(b.out).writeCString(fullName)
    b.writeI4(0) // reserved
    BsonWriter(b.out).writeDocument(query)
    
    s := db.connection.getSocket()
    db.connection.sendMsg(s.out, b.flip, Mongo.OP_DELETE)
    // todo - read last error in strict mode
  }

}
