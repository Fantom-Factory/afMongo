
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
	new makeFromQname(ConnectionManager conMgr, Str qname) {
		this.conMgr		= conMgr
		this.namespace 	= Namespace(qname)
	}

	** Creates a 'Collection' with the given name under the database.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new makeFromDatabase(Database database, Str name) {
		this.conMgr 	= database.conMgr
		this.namespace 	= Namespace(database.name, name)
	}

	internal new makeFromNamespace(ConnectionManager conMgr, Namespace namespace) {
		this.conMgr		= conMgr
		this.namespace 	= namespace
	}
	
	// ---- Collection ----------------------------------------------------------------------------

	** Returns 'true' if this collection exists.
	Bool exists() {
		Collection(conMgr, namespace.withCollection("system.namespaces")).findCount(["name": "${namespace.databaseName}.${name}"]) > 0
	}
	
	** Creates a new collection explicitly.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This create(Bool? autoIndexId := true, Bool? usePowerOf2Sizes := true) {
		flags := (usePowerOf2Sizes == null) ? null : (usePowerOf2Sizes ? 1 : 0) 
		cmd	.add("create", 		name)
			.add("autoIndexId",	autoIndexId)
			.add("flags", 		flags)
			.run
		// as create() only returns [ok:1.0], return this
		return this
	}

	** Creates a capped collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This createCapped(Int size, Int? maxNoOfDocs := null, Bool? autoIndexId := true, Bool? usePowerOf2Sizes := true) {
		flags := (usePowerOf2Sizes == null) ? null : (usePowerOf2Sizes ? 1 : 0) 
		cmd	.add("create", 		name)
			.add("capped", 		true)
			.add("size", 		size)
			.add("autoIndexId", autoIndexId)
			.add("max", 		maxNoOfDocs)
			.add("flags",		flags)
			.run
		// as create() only returns [ok:1.0], return this
		return this
	}

	** Drops this collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/drop/`
	This drop(Bool checked := true) {
		if (checked || exists) cmd.add("drop", name).run
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
	** 
	** Returns what is returned from the given cursor function.
	** 
	** pre>
	** second := collection.find([:]) |cursor| {
	**     first  := cursor.next
	**     second := cursor.next
	**     return second
	** }
	** <pre
	** 
	** @see `Cursor`
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
	[Str:Obj?]? findOne(Str:Obj? query, Bool checked := true) {
		// findOne() is optomised to NOT call count() on a successful call 
		find(query) |cursor| {
			// "If numberToReturn is 1 the server will treat it as -1 (closing the cursor automatically)."
			// Means I can't use the isAlive() trick to check for more documents.
			cursor.batchSize = 2
			one := cursor.next(false) ?: (checked ? throw MongoErr(ErrMsgs.collection_findOneIsEmpty(qname, query)) : null)
			if (cursor.isAlive || cursor.next(false) != null)
				throw MongoErr(ErrMsgs.collection_findOneHasMany(qname, cursor.count, query))
			return one
		}
	}

	** Returns the result of the given 'query' as a list of documents.
	** 
	** If 'sort' is a Str it should the name of an index to use as a hint. 
	** If 'sort' is a '[Str:Obj?]' map, it should be a sort document with field names as keys. 
	** Values may either be the standard Mongo '1' and '-1' for ascending / descending or the strings 'ASC' / 'DESC'.
	** The 'sort' map, should it contain more than 1 entry, must be ordered.
	** 
	** @see `Cursor.toList`
	[Str:Obj?][] findAll(Str:Obj? query := [:], Obj? sort := null, Int skip := 0, Int? limit := null) {
		find(query) |Cursor cursor->[Str:Obj?][]| {
			cursor.skip  = skip
			cursor.limit = limit
			if (sort is Str)	cursor.hint 	= sort
			if (sort is Map)	cursor.orderBy  = sort
			if (sort != null && sort isnot Str && sort isnot Map)
				throw ArgErr(ErrMsgs.collection_findAllSortArgBad(sort))
			return cursor.toList
		}
	}

	** Returns the number of documents that would be returned by the given 'query'.
	** 
	** @see `Cursor.count`
	Int findCount(Str:Obj? query, Int skip := 0, Int? limit := null) {
		find(query) |cur->Int| {
			cur.skip  = skip
			cur.limit = limit
			return cur.count 
		}
	}
	
	** Convenience / shorthand notation for 'findOne(["_id" : id], true)'
	@Operator
	[Str:Obj?]? get(Obj id, Bool checked := true) {
		findOne(["_id" : id], checked)
	}

	// ---- Write Operations ----------------------------------------------------------------------

	** Inserts the given document,
	** Returns the number of documents deleted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	Int insert(Str:Obj? document) {
		insertMulti([document], null)["n"]->toInt
	}

	** Inserts many documents.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	@NoDoc
	[Str:Obj?] insertMulti([Str:Obj?][] inserts, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd("insert")
			.add("insert",			name)
			.add("documents",		inserts)
			.add("ordered",			ordered)
			.add("writeConcern",	writeConcern)
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
		cmd := cmd
			.add("q",		query)
			.add("limit",	deleteAll ? 0 : 1)
		return deleteMulti([cmd.query], null)["n"]->toInt
	}

	** Executes many delete queries.
	** 	
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	@NoDoc
	[Str:Obj?] deleteMulti([Str:Obj?][] deletes, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd("delete")
			.add("delete",			name)
			.add("deletes",			deletes)
			.add("ordered",			ordered)
			.add("writeConcern",	writeConcern)
			.run
	}

	** Runs the given 'updateCmd' against documents returned by the given 'query'.
	** Returns the number of documents modified.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	// TODO: we loose any returned upserted id...?
	Int update(Str:Obj? query, Str:Obj? updateCmd, Bool? multi := false, Bool? upsert := false) {
		cmd := cmd
			.add("q",		query)
			.add("u",		updateCmd)
			.add("upsert",	upsert)
			.add("multi",	multi)
		return updateMulti([cmd.query], null)["nModified"]->toInt
	}

	** Runs multiple update queries.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	@NoDoc
	[Str:Obj?] updateMulti([Str:Obj?][] updates, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd("update")
			.add("update",			name)
			.add("updates",			updates)
			.add("ordered",			ordered)
			.add("writeConcern",	writeConcern)
			.run
	}

	** Updates and returns a single document.
	** 
	**   Options  Type  
	**   -------  ----  
	**   upsert   Bool  
	**   sort     Doc   
	**   fields   Doc
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/findAndModify/`
	[Str:Obj?] findAndUpdate(Str:Obj? query, Str:Obj? updateCmd, Bool returnModified, [Str:Obj?]? options := null) {
		cmd	.add("findAndModify",	name)
			.add("query", 			query)
			.add("update", 			updateCmd)
			.add("new", 			returnModified)
			.addAll(options)
			.run["value"]
	}

	** Updates and returns a single document.
	** 
	**   Options  Type  
	**   -------  ----  
	**   sort     Doc   
	**   fields   Doc
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
	
	** Run a map-reduce aggregation operation over the collection.
	** 
	** If 'out' is a Str, it specifies the name of a collection to store the results.
	** If 'out' is a Map, it specified the action to take. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/mapReduce/`
	[Str:Obj?] mapReduce(Str mapFunc, Str reduceFunc, Obj out, [Str:Obj?]? options := null) {
		cmd	.add("mapReduce",	name)
			.add("map", 		mapFunc)
			.add("reduce", 		reduceFunc)
			.add("out", 		out)
			.addAll(options)
			.run
	}
	
	** Performs an aggregation operation using a sequence of stage-based manipulations.
	** 
	** @see 
	**  - `http://docs.mongodb.org/manual/reference/command/aggregate/`
	**  - `http://docs.mongodb.org/manual/reference/aggregation/`
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
			try {
				return func(cursor)
			} finally {
				cursor.kill
			}
		}		
	}
	
	// ---- Indexes -------------------------------------------------------------------------------

	** Returns all the index names of this collection.
	Str[] indexNames() {
		idxNs := namespace.withCollection("system.indexes")
		return Collection(conMgr, idxNs.qname).findAll(["ns":namespace.qname]).map { it["name"] }.sort
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
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, namespace, action)
	}	
}
