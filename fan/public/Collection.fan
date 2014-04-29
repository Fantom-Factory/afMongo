
** Represents a MongoDB collection.
class Collection {
	
	private Namespace	namespace
	
	** The 'connection' this collection will use to query the database. 
	internal Connection	connection {
		private set
	}

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
	
	new makeFromQname(Connection connection, Str qname) {
		this.connection	= connection
		this.namespace 	= Namespace(qname)
	}

	new makeFromDatabase(Database database, Str name) {
		this.connection = database.conMgr.getConnection
		this.namespace 	= Namespace(database.name, name)
	}

	Void create() {
		// FIXME!
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
		cursor := Cursor(connection, namespace, query)
		try {
			return func(cursor)
		} finally {
			cursor.kill
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
			// Means I then can't use the isAlive() trick to check for more documents.
			cursor.batchSize = 2
			one := cursor.next(false) ?: (checked ? throw MongoErr(ErrMsgs.collection_findOneIsEmpty(qname, query)) : null)
			if (cursor.isAlive || cursor.next(false) != null)
				throw MongoErr(ErrMsgs.collection_findOneHasMany(qname, cursor.count, query))
			return one
		}
	}

	** Returns the result of the given 'query' as a list of documents.
	** 
	** @see `Cursor`
	[Str:Obj?][] findList(Str:Obj? query := [:], Int skip := 0, Int? limit := null) {
		find(query) |Cursor cursor->[Str:Obj?][]| {
			cursor.skip  = skip
			cursor.limit = limit
			return cursor.toList
		}
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
		runCmd(cmd.add("count", name))["n"]->toInt
	}

	** Inserts the given document,
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	Str:Obj? insert(Str:Obj? document, [Str:Obj?]? writeConcern := null) {
		insertMulti([document], null, writeConcern)
	}

	** Inserts many delete documents.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/insert/`
	@NoDoc
	Str:Obj? insertMulti([Str:Obj?][] inserts, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("insert",		name)
			.add("documents",	inserts)
		if (ordered != null)		cmd["ordered"] 		= ordered
		if (writeConcern != null)	cmd["writeConcern"] = writeConcern
		return checkForWriteErrs("inserting into", "inserted", runCmd(cmd))
	}

	** Deletes documents that match the given query.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	Str:Obj? delete(Str:Obj? query, Int limit := 0, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("q",		query)
			.add("limit",	limit)
		return deleteMulti([cmd], null, writeConcern)
	}

	** Executes many delete queries.
	** 	
	** @see `http://docs.mongodb.org/manual/reference/command/delete/`
	@NoDoc
	Str:Obj? deleteMulti([Str:Obj?][] deletes, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("delete",	name)
			.add("deletes",	deletes)
		if (ordered != null)		cmd["ordered"] 		= ordered
		if (writeConcern != null)	cmd["writeConcern"] = writeConcern
		return checkForWriteErrs("deleting from", "deleted", runCmd(cmd))
	}

	** Runs the given 'updateCmd' against documents returned by the given 'query'.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	Str:Obj? update(Str:Obj? query, Str:Obj? updateCmd, Bool? upsert := null, Bool? multi := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("q",	query)
			.add("u",	updateCmd)
		if (upsert != null)	cmd["upsert"] = upsert
		if (multi  != null)	cmd["multi"]  = multi
		return updateMulti([cmd], null, writeConcern)
	}

	** Runs multiple update queries.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/update/`
	@NoDoc
	Str:Obj? updateMulti([Str:Obj?][] updates, Bool? ordered := null, [Str:Obj?]? writeConcern := null) {
		cmd := cmd
			.add("update",	name)
			.add("updates",	updates)
		if (ordered != null)		cmd["ordered"] 		= ordered
		if (writeConcern != null)	cmd["writeConcern"] = writeConcern
		return checkForWriteErrs("updating", "updated", runCmd(cmd))
	}

//	http://docs.mongodb.org/manual/reference/command/findAndModify/#dbcmd.findAndModify
	// TODO: findAndDelete findAndUpdate
//	findAndDelete()
//	findAndUpdate()
	
	** Drops this collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/drop/`
	This drop() {
		runCmd(["drop":name])
		return this
	}
	
	Indexes indexes() {
		Indexes(connection, namespace)
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Str:Obj? cmd() {
		Str:Obj?[:] { ordered = true }
	}	
	
	private Str:Obj? runCmd(Str:Obj? cmd) {
		Operation(connection).runCommand("${namespace.databaseName}.\$cmd", cmd)
	}

	private Str:Obj? runAdminCmd(Str:Obj? cmd) {
		Operation(connection).runCommand("admin.\$cmd", cmd)
	}
	
	private Str:Obj? checkForWriteErrs(Str what, Str past, Str:Obj? doc) {
		errs := [Str:Obj?][,]
		if (doc.containsKey("writeErrors"))
			errs.addAll((Obj?[]) doc["writeErrors"])
		if (doc.containsKey("writeConcernError"))
			errs.add((Str:Obj?) doc["writeConcernError"])
		if (!errs.isEmpty)
			throw MongoCmdErr(ErrMsgs.collection_writeErrs(what, qname, errs))
		if (doc["n"]?->toInt == 0)
			// TODO: have a 'checked' variable?
			throw MongoErr(ErrMsgs.collection_nothingHappened(past, doc))
		return doc
	}
}
