//using afBson::Code

** Represents a MongoDB collection.
** 
** https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#insert-update-replace-delete-and-bulk-writes
const class MongoCol {
	
//	private const Namespace	namespace
	
	** The underlying connection manager.
	const MongoConnMgr connMgr
	
	const Str dbName

	** The simple name of the collection.
	const Str name

	** Creates a 'Collection' with the given name under the database.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new makeFromDatabase(MongoConnMgr connMgr, Str dbName, Str name) {
		this.connMgr 	= connMgr
		this.dbName		= MongoDb.validateName(dbName)
		this.name 		= validateName(name)
	}
	
	
	internal static Str validateName(Str name) {
		if (name.isEmpty)
			throw ArgErr("Collection name can not be empty")
		if (name.any { it == '$' })
			throw ArgErr("Collection name '${name}' may not contain any of the following: \$")
		return name
	}
	
	// ---- Collection ----------------------------------------------------------------------------

	** Returns 'true' if this collection exists.
	Bool exists() {
		res := cmd("listCollections", 1).add("filter", ["name":name]).run
		return res["cursor"]->get("firstBatch")->isEmpty->not
	}
	
	** Creates a new collection explicitly.
	** 
	** There is usually no no need to call this unless you wish explicitly set collection options. 
	**  
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This create([Str:Obj?]? options := null) {
		cmd		("create", name)
			.addAll(options)
			.run
		// as create() only returns [ok:1.0], return this
		return this
	}

	** Creates a capped collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This createCapped(Int sizeInBytes, Int? maxNoOfDocs := null, [Str:Obj?]? options := null) {
		cmd		("create", 		name)
			.add("capped", 		true)
			.add("size", 		sizeInBytes)
			.add("max", 		maxNoOfDocs)
			.addAll(options)
			.run
		// as create() only returns [ok:1.0], return this
		return this
	}

	** Drops this collection, but only if it exists.
	** 
	** Note that deleting all documents is MUCH quicker than dropping the Collection. See `deleteAll` for details.
	** 
	** If 'force' is 'true' then no checks are made. 
	** This will result in an error if the collection doesn't exist.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/drop/`
	This drop(Bool force := false) {
		if (force || exists) cmd("drop", name).run
		// [ns:afMongoTest.col-test, nIndexesWas:1, ok:1.0] 
		// not sure wot 'nIndexesWas' or if it's useful, so return this for now 
		return this
	}

	// ---- Diagnostics  --------------------------------------------------------------------------
	
	** Returns storage statistics for this collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/collStats/`
	[Str:Obj?] stats(Int scale := 1) {
		cmd("collStats", name).add("scale", scale).run
	}

	// ---- Cursor Queries ------------------------------------------------------------------------

	** Creates a `Cursor` over the given 'query' allowing you to iterate over results.
	** Documents are downloaded from MongoDB in batches behind the scene as and when required. 
	** Use 'find()' to optomise iterating over a *massive* result set. 
	** 
	** Returns what is returned from the given cursor function.
	** 
	** pre>
	** syntax: fantom
	** 
	** second := collection.find([:]) |cursor->Obj?| {
	**     first  := cursor.next
	**     second := cursor.next
	**     return second
	** }
	** <pre
	** 
	**  - @see `Cursor`
	**  - @see `http://docs.mongodb.org/manual/reference/operator/query/`
	Obj? find(Str:Obj? query) {
		
		// FIXME
//		throw UnsupportedErr()
		connMgr.leaseConn |con->Obj?| {
			query["find"] = name
//			query["singleBatch"] = true
			query["batchSize"] = 2
			
			res := MongoOp(con).runCommand(dbName, query)

			return res
		}
		
//		connMgr.leaseConnection |con->Obj?| {
//			cursor := Cursor(con, namespace, query)
//			try {
//				return func(cursor)
//			} finally {
//				cursor.kill
//			}
//		}
	}
	
	MongoCur findCur(Str:Obj? filter) {
		cmd("find", name)
			.add("filter", filter)
			.add("batchSize", 2)
			.cursor
		// FIXME set batchSize and timeout on cursor
	}

	** An (optomised) method to return one document from the given 'query'.
	** 
	** Throws 'MongoErr' if no documents are found and 'checked' is true, returns 'null' otherwise.
	** Always throws 'MongoErr' if the query returns more than one document.
	**  
	** @see `http://docs.mongodb.org/manual/reference/operator/query/`
	[Str:Obj?]? findOne([Str:Obj?]? query := null, Bool checked := true) {
		
		throw UnsupportedErr()
		
//		query = query ?: Str:Obj?[:]
//		// findOne() is optomised to NOT call count() on a successful call 
//		return find(query) |cursor| {
//			// "If numberToReturn is 1 the server will treat it as -1 (closing the cursor automatically)."
//			// Means I can't use the isAlive() trick to check for more documents.
//			cursor.batchSize = 2
//			one := cursor.next(false) ?: (checked ? throw MongoErr(MongoErrMsgs.collection_findOneIsEmpty(qname, query)) : null)
//			if (cursor.isAlive || cursor.next(false) != null)
//				throw MongoErr(MongoErrMsgs.collection_findOneHasMany(qname, cursor.count, query))
//			return one
//		}
	}

	** Returns the result of the given 'query' as a list of documents.
	** 
	** If 'sort' is a Str it should the name of an index to use as a hint. 
	** If 'sort' is a '[Str:Obj?]' map, it should be a sort document with field names as keys. 
	** Values may either be the standard Mongo '1' and '-1' for ascending / descending or the 
	** strings 'ASC' / 'DESC'.
	** 
	** The 'sort' map, should it contain more than 1 entry, must be ordered.
	** 
	** 'projection' alters / limits which fields returned in the query results.
	** 
	** Note that 'findAll(...)' is a convenience for calling 'find(...)' and returning the cursor as a list. 
	** 
	** - @see `Cursor.toList`
	** - @see `http://docs.mongodb.org/manual/reference/operator/query/`
	** - @see `https://docs.mongodb.com/manual/reference/operator/projection/`
	[Str:Obj?][] findAll([Str:Obj?]? query := null, Obj? sort := null, Int skip := 0, Int? limit := null, [Str:Obj?]? projection := null) {
		
//		find(query ?: Str:Obj?[:] { it.ordered = true})
		
		
		connMgr.leaseConn |con->Obj?| {
			query = query ?: Str:Obj?[:] { it.ordered = true}
			query["find"] = name
			query["singleBatch"] = true
			
			res := MongoOp(con).runCommand(dbName, query)

			return res->get("cursor")->get("firstBatch")
		}
	
//		throw UnsupportedErr()

//		query = query ?: Str:Obj?[:] { it.ordered = true }
//		return find(query) |Cursor cursor->[Str:Obj?][]| {
//			cursor.skip  		= skip
//			cursor.limit 		= limit
//			cursor.projection	= projection
//			if (sort is Str)	cursor.hint 	= sort
//			if (sort is Map)	cursor.orderBy  = sort
//			if (sort != null && sort isnot Str && sort isnot Map)
//				throw ArgErr(MongoErrMsgs.collection_findAllSortArgBad(sort))
//			return cursor.toList
//		}
	}

	** Returns the number of documents that would be returned by the given 'query'.
	** 
	** @see `Cursor.count`
	Int findCount([Str:Obj?]? query := null) {
		throw UnsupportedErr()
//		query = query ?: Str:Obj?[:]
//		return find(query) |cur->Int| {
//			cur.count
//		}
	}
	
	** Convenience / shorthand notation for 'findOne(["_id" : id], checked)'
	@Operator
	[Str:Obj?]? get(Obj? id, Bool checked := true) {
		if (id == null)
			return !checked ? null : (null ?: throw Err(MongoErrMsgs.collection_findOneIsEmpty(qname, id)))
		return findOne(["_id" : id], checked)
	}

	// ---- Write Operations ----------------------------------------------------------------------

	** Inserts the given document.
	** Returns the number of documents inserted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	Int insert(Str:Obj? document) {
		insertMulti([document], null)["n"]->toInt
	}

	** Inserts multiple documents.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	[Str:Obj?] insertMulti([Str:Obj?][] inserts, Bool? ordered := null) {
		cmd		("insert",			name)
			.add("documents",		inserts)
			.add("ordered",			ordered)
			.add("writeConcern",	connMgr.writeConcern)
			.run
	}

	** Deletes documents that match the given query.
	** Returns the number of documents deleted.
	** 
	** If 'deleteAll' is 'true' then all documents matching the query will be deleted, otherwise 
	** only the first match will be deleted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	Int delete(Str:Obj? query, Bool deleteAll := false) {
		cmd := 
			cmd ("q",		query)
			.add("limit",	deleteAll ? 0 : 1)
		return deleteMulti([cmd.cmd], null)["n"]->toInt
	}

	** Executes multiple delete queries.
	** 	
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	[Str:Obj?] deleteMulti([Str:Obj?][] deletes, Bool? ordered := null) {
		cmd		("delete",			name)
			.add("deletes",			deletes)
			.add("ordered",			ordered)
			.add("writeConcern",	connMgr.writeConcern)
			.run
	}
	
	** Convenience method for deleting ALL documents in a Collection.
	** Returns the number of documents deleted.
	** 
	** Note this is MUCH quicker than dropping the Collection.
	** 
	** Same as calling:
	** 
	**   syntax: fantom
	**   deleteMulti([["q":[:], "limit":0]], false)["n"]
	Int deleteAll() {
		deleteMulti([["q":[:], "limit":0]], false)["n"]->toInt
	}

	** Runs the given 'updateCmd' against documents returned by 'query'.
	** Inspect return value for upserted IDs.
	** Note this does *not* throw an Err should the query not match any documents.
	** 
	** If 'multi' is 'true' then the multiple documents may be updated, otherwise the update is limited to one.
	** 
	** If 'upsert' is 'true' and no documents are updated, then one is inserted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	[Str:Obj?] update(Str:Obj? query, Str:Obj? updateCmd, Bool? multi := false, Bool? upsert := false) {
		cmd := 
			 cmd("q",		query)
			.add("u",		updateCmd)
			.add("upsert",	upsert)
			.add("multi",	multi)
		return updateMulti([cmd.cmd], null)
	}

	** Runs multiple update queries.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	[Str:Obj?] updateMulti([Str:Obj?][] updates, Bool? ordered := null) {
		cmd		("update",			name)
			.add("updates",			updates)
			.add("ordered",			ordered)
			.add("writeConcern",	connMgr.writeConcern)
			.run
	}

	** Updates and returns a single document. 
	** If the query returns multiple documents then the first one is updated.
	** 
	** If 'returnModified' is 'true' then the document is returned *after* the updates have been applied.
	** 
	** Returns 'null' if no document was found.
	** 
	** The 'options' parameter is merged with the Mongo command and may contain the following:
	** 
	**   table:
	**   Options  Type  Desc
	**   -------  ----  ----
	**   upsert   Bool  Creates a new document if no document matches the query
	**   sort     Doc   Orders the result to determine which document to update.
	**   fields   Doc   Defines which fields to return.
	** 
	** Example:
	** 
	**   syntax: fantom
	**   collection.findAndUpdate(query, cmd, true, ["upsert":true, "fields": ["myEntity.myField":1]]
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/findAndModify/`
	[Str:Obj?]? findAndUpdate(Str:Obj? query, Str:Obj? updateCmd, Bool returnModified, [Str:Obj?]? options := null) {
		cmd		("findAndModify",	name)
			.add("query", 			query)
			.add("update", 			updateCmd)
			.add("new", 			returnModified)
			.addAll(options)
			.run["value"]
	}

	** Deletes and returns a single document.
	** If the query returns multiple documents then the first one is delete.
	** 
	** The 'options' parameter is merged with the Mongo command and may contain the following:
	** 
	**   table:
	**   Options  Type  Desc  
	**   -------  ----  ----
	**   sort     Doc   Orders the result to determine which document to delete.
	**   fields   Doc   Defines which fields to return.
	** 
	** Example:
	** 
	**   syntax: fantom
	**   collection.findAndDelete(query, ["fields": ["myEntity.myField":1]]
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/findAndModify/`
	[Str:Obj?] findAndDelete(Str:Obj? query, [Str:Obj?]? options := null) {
		cmd		("findAndModify",	name)
			.add("query", 			query)
			.add("remove", 			true)
			.addAll(options)
			.run["value"]
	}	
	
	// ---- Aggregation Commands ------------------------------------------------------------------
	
	** Returns the number of documents in the collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/count/`
	Int size() {
		cmd("count", name).run["n"]->toInt
	}

	** @see 
	**  - `http://docs.mongodb.org/manual/reference/command/aggregate/`
	**  - `http://docs.mongodb.org/manual/reference/aggregation/`
	Obj? aggregate([Str:Obj?][] pipeline, |Cursor->Obj?| func) {
		
		// FIXME
		throw UnsupportedErr()
		
//		cmd := cmd
//			.add("aggregate",	name)
//			.add("pipeline", 	pipeline)
//			.add("cursor",		["batchSize": 0])
//
//		results	 := (Str:Obj?) cmd.run["cursor"]
//		cursorId := results["id"]
//		firstBat := results["firstBatch"]
//
//		return connMgr.leaseConnection |con->Obj?| {
//			cursor := Cursor(con, namespace, cmd.query, cursorId, firstBat)
//			try return	func(cursor)
//			finally		cursor.kill
//		}
	}
	
	// ---- Indexes -------------------------------------------------------------------------------

	** Returns all the index names of this collection.
	Str[] indexNames() {
		res := cmd("listIndexes", name).run
		nfo := ([Str:Obj?][]) res["cursor"]->get("firstBatch")
		return nfo.map |i->Str| { i["name"] }
	}
	
	** Returns an 'MongoIndex' of the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDb. 
	MongoIndex index(Str indexName) { 
		MongoIndex(connMgr, dbName, name, indexName)
	}

	** Drops ALL indexes on the collection. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	This dropAllIndexes() {
		cmd("dropIndexes", name).add("index", "*").run
		// [nIndexesWas:2, ok:1.0]
		return this
	}
	
	// ---- Misc Methods --------------------------------------------------------------------------
	
//	** Runs an arbitrary command against this 'Collection'. 
//	** Example, to return the size of the collection:
//	** 
//	**   size := runCmd(
//	**     ["count" : "<collectionName>"]
//	**   )["n"]->toInt
//	** 
//	** *This is a low level operation.*
//	** 
//	** See `https://docs.mongodb.com/manual/reference/command/`  
//	Str:Obj? runCmd(Str:Obj? query) {
//		cmd.addAll(query).run
//	}
	
	Str qname() {
		"${dbName}.${name}"
	}
		
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		qname
	}

	// ---- Private Methods -----------------------------------------------------------------------
	
	** **For Power Users!**
	** 
	** Don't forget to call 'run()'!
	private MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, dbName, cmdName, cmdVal)
	}
}
