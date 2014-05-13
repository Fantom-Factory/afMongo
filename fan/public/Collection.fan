
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

	** Returns 'true' if this collection exists.
	Bool exists() {
		Collection(conMgr, namespace.withCollection("system.namespaces")).findCount(["name": "${namespace.databaseName}.${name}"]) > 0
	}
	
	** Creates a new collection explicitly.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This create(Bool? autoIndexId := true, Bool? usePowerOf2Sizes := true) {
		cmd := cmd.add("create", name)
		if (autoIndexId != null)		cmd.add("autoIndexId", autoIndexId)
		if (usePowerOf2Sizes != null)	cmd.add("flags", usePowerOf2Sizes ? 1 : 0)
		cmd.run
		// as create() only returns [ok:1.0], return this
		return this
	}

	** Creates a capped collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/create/`
	This createCapped(Int size, Int? maxNoOfDocs := null, Bool? autoIndexId := null, Bool? usePowerOf2Sizes := null) {
		cmd := cmd.add("create", name).add("capped", true).add("size", size)
		if (autoIndexId != null)		cmd.add("autoIndexId", autoIndexId)
		if (maxNoOfDocs != null)		cmd.add("max", maxNoOfDocs)
		if (usePowerOf2Sizes != null)	cmd.add("flags", usePowerOf2Sizes ? 1 : 0)
		cmd.run
		// as create() only returns [ok:1.0], return this
		return this
	}
	
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
	** @see `Cursor`
	[Str:Obj?][] findAll(Str:Obj? query := [:], Obj? sort := null, Int skip := 0, Int? limit := null) {
		find(query) |Cursor cursor->[Str:Obj?][]| {
			cursor.skip  = skip
			cursor.limit = limit
			if (sort is Str)	cursor.hint 	= sort
			if (sort is Map)	cursor.orderBy  = sort
			if (sort isnot Str && sort isnot Map)
				throw ArgErr(ErrMsgs.collection_findAllSortArgBad(sort))
			return cursor.toList
		}
	}
	
	** Convenience / shorthand notation for 'findOne(["_id" : id], true)'
	@Operator
	[Str:Obj?] get(Obj id) {
		findOne(["_id" : id], true)
	}

	** Returns the number of documents that would be returned by the given 'query'.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/count/`
	Int findCount(Str:Obj? query) {
		find(query) { it.count }
	}

	** Returns the number of documents in the collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/count/`
	Int size() {
		cmd.add("count", name).run["n"]->toInt
	}

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
	Str:Obj? insertMulti([Str:Obj?][] inserts, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd("insert")
			.add("insert",		name)
			.add("documents",	inserts)
		if (ordered != null)		cmd["ordered"] 		= ordered
		if (writeConcern != null)	cmd["writeConcern"] = writeConcern
		return cmd.run
	}

	** Deletes documents that match the given query.
	** Returns the number of documents deleted.
	** 
	** If 'deleteAll' is 'true' then all documents matching the query will be deleted, otherwise 
	** only the first match will be deleted.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	Int delete(Str:Obj? query, Bool deleteAll := false) {
		cmd := [Str:Obj?][:] { ordered = true }
			.add("q",		query)
			.add("limit",	deleteAll ? 0 : 1)
		return deleteMulti([cmd], null)["n"]->toInt
	}

	** Executes many delete queries.
	** 	
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	@NoDoc
	Str:Obj? deleteMulti([Str:Obj?][] deletes, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd("delete")
			.add("delete",	name)
			.add("deletes",	deletes)
		if (ordered != null)		cmd["ordered"] 		= ordered
		if (writeConcern != null)	cmd["writeConcern"] = writeConcern
		return cmd.run
	}

	** Runs the given 'updateCmd' against documents returned by the given 'query'.
	** Returns the number of documents modified.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	// TODO: we loose any returned upserted id...?
	Int update(Str:Obj? query, Str:Obj? updateCmd, Bool? multi := false, Bool? upsert := false) {
		cmd := [Str:Obj?][:] { ordered = true }
			.add("q",	query)
			.add("u",	updateCmd)
		if (upsert != null)	cmd["upsert"] = upsert
		if (multi  != null)	cmd["multi"]  = multi
		return updateMulti([cmd], null)["nModified"]->toInt
	}

	** Runs multiple update queries.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	@NoDoc
	Str:Obj? updateMulti([Str:Obj?][] updates, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd("update")
			.add("update",	name)
			.add("updates",	updates)
		if (ordered != null)		cmd["ordered"] 		= ordered
		if (writeConcern != null)	cmd["writeConcern"] = writeConcern
		return cmd.run
	}

//	http://docs.mongodb.org/manual/reference/command/findAndModify/#dbcmd.findAndModify
	// TODO: findAndDelete findAndUpdate
//	findAndDelete()
//	findAndUpdate()
	
	** Drops this collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/drop/`
	This drop() {
		cmd.add("drop", name).run
		// [ns:afMongoTest.col-test, nIndexesWas:1, ok:1.0] 
		// not sure wot 'nIndexesWas' or if it's useful, so return this for now 
		return this
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
