using afBson::Code

** Represents a MongoDB collection.
const class Collection {
	
	private const Namespace	namespace
	
	internal const ConnectionManager conMgr

	** The qualified name of the collection. 
	** It takes the form of: 
	** 
	**   <database>.<collection>
	Str qname {
		get { namespace.qname }
		private set { }
	}

	** The simple name of the collection.
	Str name {
		get { namespace.collectionName }
		private set { }
	}
	
	** Creates a 'Collection' with the given qualified (dot separated) name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new makeFromQname(ConnectionManager conMgr, Str qname, |This|? f := null) {
		f?.call(this)
		this.conMgr		= conMgr
		this.namespace 	= Namespace(qname)
	}

	** Creates a 'Collection' with the given name under the database.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new makeFromDatabase(Database database, Str name, |This|? f := null) {
		f?.call(this)
		this.conMgr 	= database.conMgr
		this.namespace 	= Namespace(database.name, name)
	}

	internal new makeFromNamespace(ConnectionManager conMgr, Namespace namespace, |This|? f := null) {
		f?.call(this)
		this.conMgr		= conMgr
		this.namespace 	= namespace
	}
	
	// ---- Collection ----------------------------------------------------------------------------

	** Returns 'true' if this collection exists.
	Bool exists() {
		res := cmd.add("listCollections", 1).add("filter", ["name":name]).run
		return res["cursor"]->get("firstBatch")->isEmpty->not
	}
	
	** Creates a new collection explicitly.
	** 
	** There is usually no no need to call this unless you wish explicitly set collection options. 
	**  
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This create([Str:Obj?]? options := null) {
		cmd	.add("create", name)
			.addAll(options)
			.run
		// as create() only returns [ok:1.0], return this
		return this
	}

	** Creates a capped collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This createCapped(Int sizeInBytes, Int? maxNoOfDocs := null, [Str:Obj?]? options := null) {
		cmd	.add("create", 		name)
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
		if (force || exists) cmd.add("drop", name).run
		// [ns:afMongoTest.col-test, nIndexesWas:1, ok:1.0] 
		// not sure wot 'nIndexesWas' or if it's useful, so return this for now 
		return this
	}

	// ---- Diagnostics  --------------------------------------------------------------------------
	
	** Returns storage statistics for this collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/collStats/`
	[Str:Obj?] stats(Int scale := 1) {
		cmd.add("collStats", name).add("scale", scale).run
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
	Obj? find(Str:Obj? query, |Cursor->Obj?| func) {
		conMgr.leaseConnection |con->Obj?| {
			cursor := Cursor(con, namespace, query)
			try {
				return func(cursor)
			} finally {
				cursor.kill
			}
		}
	}

	** An (optomised) method to return one document from the given 'query'.
	** 
	** Throws 'MongoErr' if no documents are found and 'checked' is true, returns 'null' otherwise.
	** Always throws 'MongoErr' if the query returns more than one document.
	**  
	** @see `http://docs.mongodb.org/manual/reference/operator/query/`
	[Str:Obj?]? findOne([Str:Obj?]? query := null, Bool checked := true) {
		query = query ?: Str:Obj?[:]
		// findOne() is optomised to NOT call count() on a successful call 
		return find(query) |cursor| {
			// "If numberToReturn is 1 the server will treat it as -1 (closing the cursor automatically)."
			// Means I can't use the isAlive() trick to check for more documents.
			cursor.batchSize = 2
			one := cursor.next(false) ?: (checked ? throw MongoErr(MongoErrMsgs.collection_findOneIsEmpty(qname, query)) : null)
			if (cursor.isAlive || cursor.next(false) != null)
				throw MongoErr(MongoErrMsgs.collection_findOneHasMany(qname, cursor.count, query))
			return one
		}
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
		query = query ?: Str:Obj?[:]
		return find(query) |Cursor cursor->[Str:Obj?][]| {
			cursor.skip  		= skip
			cursor.limit 		= limit
			cursor.projection	= projection
			if (sort is Str)	cursor.hint 	= sort
			if (sort is Map)	cursor.orderBy  = sort
			if (sort != null && sort isnot Str && sort isnot Map)
				throw ArgErr(MongoErrMsgs.collection_findAllSortArgBad(sort))
			return cursor.toList
		}
	}

	** Returns the number of documents that would be returned by the given 'query'.
	** 
	** @see `Cursor.count`
	Int findCount([Str:Obj?]? query := null) {
		query = query ?: Str:Obj?[:]
		return find(query) |cur->Int| {
			cur.count
		}
	}
	
	** Convenience / shorthand notation for 'findOne(["_id" : id], checked)'
	@Operator
	[Str:Obj?]? get(Obj? id, Bool checked := true) {
		if (id == null)
			return !checked ? null : (null ?: throw MongoErr(MongoErrMsgs.collection_findOneIsEmpty(qname, id)))
		return findOne(["_id" : id], checked)
	}

	// ---- Write Operations ----------------------------------------------------------------------

	** Inserts the given document.
	** Returns the number of documents inserted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	Int insert(Str:Obj? document, [Str:Obj?]? writeConcern := null) {
		insertMulti([document], null, writeConcern)["n"]->toInt
	}

	** Inserts multiple documents.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	[Str:Obj?] insertMulti([Str:Obj?][] inserts, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd("insert")
			.add("insert",			name)
			.add("documents",		inserts)
			.add("ordered",			ordered)
			.add("writeConcern",	writeConcern ?: conMgr.writeConcern)
			.run
	}

	** Deletes documents that match the given query.
	** Returns the number of documents deleted.
	** 
	** If 'deleteAll' is 'true' then all documents matching the query will be deleted, otherwise 
	** only the first match will be deleted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	Int delete(Str:Obj? query, Bool deleteAll := false, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("q",		query)
			.add("limit",	deleteAll ? 0 : 1)
		return deleteMulti([cmd.query], null, writeConcern)["n"]->toInt
	}

	** Executes multiple delete queries.
	** 	
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	[Str:Obj?] deleteMulti([Str:Obj?][] deletes, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd("delete")
			.add("delete",			name)
			.add("deletes",			deletes)
			.add("ordered",			ordered)
			.add("writeConcern",	writeConcern ?: conMgr.writeConcern)
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
	**   deleteMulti([["q":[:], "limit":0]], false, writeConcern)["n"]
	Int deleteAll([Str:Obj?]? writeConcern := null) {
		deleteMulti([["q":[:], "limit":0]], false, writeConcern)["n"]->toInt
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
	[Str:Obj?] update(Str:Obj? query, Str:Obj? updateCmd, Bool? multi := false, Bool? upsert := false, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("q",		query)
			.add("u",		updateCmd)
			.add("upsert",	upsert)
			.add("multi",	multi)
		return updateMulti([cmd.query], null, writeConcern)
	}

	** Runs multiple update queries.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	[Str:Obj?] updateMulti([Str:Obj?][] updates, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd("update")
			.add("update",			name)
			.add("updates",			updates)
			.add("ordered",			ordered)
			.add("writeConcern",	writeConcern ?: conMgr.writeConcern)
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
		cmd	.add("findAndModify",	name)
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
		cmd	.add("findAndModify",	name)
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
		cmd.add("count", name).run["n"]->toInt
	}

	** Finds the distinct values for a specified field.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/distinct/`
	Obj[] distinct(Str field, [Str:Obj?]? query := null) {
		cmd	.add("distinct",	name)
			.add("key", 		field)
			.add("query",		query)
			.run["values"]
	}

	** Groups documents by the specified key and performs simple aggregation functions. 
	** 
	** 'key' must either be a list of field names ( 'Str[]' ) or a function that creates a 
	** "key object" ( 'Str' ).  
	**
	** The 'options' parameter is merged with the Mongo command and may contain the following:
	** 
	**   table:
	**   Options   Type  Desc
	**   -------   ----  ----
	**   cond      Doc   Determines which documents in the collection to process.
	**   finalize  Func  Runs on each item in the result set before the final value is returned.
	**  
	** @see `http://docs.mongodb.org/manual/reference/command/group/`
	[Str:Obj?][] group(Obj key, [Str:Obj?] initial, Code reduceFunc, [Str:Obj?]? options := null) {
		keydoc := ([Str:Obj?]?) null; keyf := (Str?) null
		if (key is List)  { keydoc  = cmd.query.addList(key); keydoc.keys.each { keydoc[it] = 1 } }
		if (key is Str)		keyf 	= key
		if (keydoc == null && keyf == null)
			throw ArgErr(MongoErrMsgs.collection_badKeyGroup(key))

		group := cmd
			.add("ns",			name)
			.add("key",			keydoc)
			.add("\$keyf",		keyf)
			.add("initial",		initial)
			.add("\$reduce",	reduceFunc)
			.addAll(options)
		
		return cmd.add("group", group.query).run["retval"]
	}
	
	** Run a map-reduce aggregation operation over the collection.
	** 
	** If 'out' is a Str, it specifies the name of a collection to store the results.
	** If 'out' is a Map, it specifies the action to take. 
	** 
	** The 'options' parameter is merged with the Mongo command and may contain the following:
	** 
	**   table:
	**   Options   Type  Desc  
	**   -------   ----  ----
	**   query     Doc   The selection criteria for input documents.
	**   sort      Doc   Sorts the input documents.
	**   limit     Int   The maximum number of documents given to the map function.
	**   finalize  Func  Follows the 'reduce' method and modifies the output.
	**   scope     Doc   global variables used in the 'map', 'reduce' and 'finalize' functions.
	**   out       Obj   If a 'Str' then it's the name of a collection to store the results in, if a Doc, then it specifies the action to take.
	**   jsMode    Bool  If 'false' (default) objects from the 'map' function are converted into BSON before being handed to the 'finalize' function.
	**   verbose   Bool  If 'true' (default) then timing information is returned in the result.
	** 
	** Note if an 'out' option is not specified, it is taken to be *inline* and the returned document contains the results.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/mapReduce/`
	[Str:Obj?] mapReduce(Code mapFunc, Code reduceFunc, [Str:Obj?]? options := null) {
		opts := options.dup.rw
		if (!opts.containsKey("out"))
			opts["out"] = ["inline" : 1]
		return cmd
			.add("mapReduce",	name)
			.add("map", 		mapFunc)
			.add("reduce", 		reduceFunc)
			.addAll(opts)
			.run
	}
	
	** Performs an aggregation operation using a sequence of stage-based manipulations.
	** 
	** The 'options' parameter is merged with the Mongo command and may contain the following:
	**
	**   table: 
	**   Options       Type  Desc  
	**   -------       ----  ----
	**   explain       Bool  Returns pipeline processing information.
	**   allowDiskUse  Bool  If 'true' allows temp data to be stored on disk.
	**   cursor        Doc   Controls the cursor creation.
	** 
	** @see 
	**  - `http://docs.mongodb.org/manual/reference/command/aggregate/`
	**  - `http://docs.mongodb.org/manual/reference/aggregation/`
	@Deprecated { msg="Removed in MongoDB v3.6.x - instead use: aggregateCursor(pipeline) |cur| { cur.toList }" }
	[Str:Obj?][] aggregate([Str:Obj?][] pipeline, [Str:Obj?]? options := null) {
		cmd	.add("aggregate",	name)
			.add("pipeline", 	pipeline)
			.addAll(options)
			.run["result"]
	}

	** Same as 'aggregate()' but returns a cursor to iterate over the results.
	** 
	** @see 
	**  - `http://docs.mongodb.org/manual/reference/command/aggregate/`
	**  - `http://docs.mongodb.org/manual/reference/aggregation/`
	Obj? aggregateCursor([Str:Obj?][] pipeline, |Cursor->Obj?| func) {
		cmd := cmd
			.add("aggregate",	name)
			.add("pipeline", 	pipeline)
			.add("cursor",		["batchSize": 0])

		results	 := (Str:Obj?) cmd.run["cursor"]
		cursorId := results["id"]
		firstBat := results["firstBatch"]

		return conMgr.leaseConnection |con->Obj?| {
			cursor := Cursor(con, namespace, cmd.query, cursorId, firstBat)
			try return	func(cursor)
			finally		cursor.kill
		}
	}
	
	// ---- Indexes -------------------------------------------------------------------------------

	** Returns all the index names of this collection.
	Str[] indexNames() {
		res := cmd.add("listIndexes", name).run
		nfo := ([Str:Obj?][]) res["cursor"]->get("firstBatch")
		return nfo.map |i->Str| { i["name"] }
	}
	
	** Returns an 'Index' of the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	Index index(Str indexName) {
		Index(conMgr, namespace, indexName)
	}

	** Drops ALL indexes on the collection. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	This dropAllIndexes() {
		cmd.add("dropIndexes", name).add("index", "*").run
		// [nIndexesWas:2, ok:1.0]
		return this
	}
	
	// ---- Misc Methods --------------------------------------------------------------------------
	
	** Runs an arbitrary command against this 'Collection'. 
	** Example, to return the size of the collection:
	** 
	**   size := runCmd(
	**     ["count" : "<collectionName>"]
	**   )["n"]->toInt
	** 
	** *This is a low level operation.*
	** 
	** See `https://docs.mongodb.com/manual/reference/command/`  
	Str:Obj? runCmd(Str:Obj? query) {
		cmd.addAll(query).run
	}
		
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		namespace.qname
	}

	// ---- Private Methods -----------------------------------------------------------------------
	
	private MongoCmd cmd(Str? action := null) {
		MongoCmd(conMgr, namespace, action)
	}	
}
