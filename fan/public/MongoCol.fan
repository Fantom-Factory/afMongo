//using afBson::Code

** Represents a MongoDB collection.
** 
** @see `https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst`
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
	new make(MongoConnMgr connMgr, Str dbName, Str name) {
		this.connMgr 	= connMgr
		this.dbName		= MongoDb.validateName(dbName)
		this.name 		= validateName(name)
	}
	
	** Convenience / shorthand notation for 'findOne(["_id" : id], checked)'
	@Operator
	[Str:Obj?]? get(Obj? id, Bool checked := true) {
		if (id == null)		// quit early if ID is null
			return checked ? (null ?: throw Err("findOne() returned ZERO documents from ${qname} - [_id:${id}]")) : null
		return findOne(["_id" : id], checked)
	}	
	
	
	
	// ---- Indexes -----------------------------
		
	** Returns an 'MongoIndex' of the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	MongoIndex index(Str indexName) { 
		MongoIndex(connMgr, dbName, name, indexName)
	}
	
	** Returns all the indexes in this collection.
	MongoCur listIndexes() {
		cmd("listIndexes", name).cursor
	}

	** Returns all the index names in this collection.
	Str[] listIndexNames() {
		listIndexes.toList.map { it["name"] }
	}

	** Drops ALL indexes on the collection. *Be careful!*
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/dropIndexes/`
	Void dropAllIndexes() {
		cmd("dropIndexes", name).add("index", "*").run
	}
	
	
	
	// ---- Commands ----------------------------

	** Returns 'true' if this collection exists.
	Bool exists() {
		MongoDb(connMgr, dbName)
			.cmd("listCollections")
			.add("filter", ["name":name])
			.add("nameOnly", true)
			.cursor
			.toList.size > 0
	}

	** Creates a new collection explicitly.
	** 
	** There is usually no no need to call this unless you wish explicitly set collection options. 
	**  
	** pre>
	** syntax: fantom
	** db.collection("name").create {
	**   it->capped = true
	**   it->size   = 64 * 1024
	** }
	** <pre
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/create/`
	Void create(|MongoCmd cmd|? optsFn := null) {
		cmd("create", name)
			.add("writeConcern",	connMgr.writeConcern)
			.withFn(				optsFn)
			.run
	}

	** Drops this collection, but only if it exists.
	** 
	** Note that deleting all documents is MUCH quicker than dropping the Collection.
	** See `deleteAll` for details.
	** 
	** If 'force' is 'true' then no checks are made. 
	** This will result in an error if the collection does not exist.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/drop/`
	Void drop(Bool force := false) {
		if (force || exists)
			cmd("drop", name)
				.add("writeConcern", connMgr.writeConcern)
				.run
	}

	** Inserts the given document.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/insert/`
	Void insert(Str:Obj? document) {
		cmd("insert",			name)
			.add("documents",	[document])
			.add("writeConcern", connMgr.writeConcern)
			.run	
	}

	** Inserts many documents.
	** 
	** Default behaviour is to stop when inserting fails. See 'ordered' option for details.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/insert/`
	Void insertMany([Str:Obj?][] documents, |MongoCmd cmd|? optsFn := null) {
		// the driver spec says I MUST raise this error!
		// https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#insert-update-replace-delete-and-bulk-writes
		if (documents.isEmpty)
			throw ArgErr("Documents MUST not be empty.")
		cmd("insert",				name)
			.add("documents",		documents)
			.add("writeConcern",	connMgr.writeConcern)
			.withFn(				optsFn)
			.run
	}
	
	** Return one document that matches the given 'filter'.
	** 
	** Throws an 'Err' if no documents are found and 'checked' is 'true'.
	** 
	** Always throws 'Err' if the filter returns more than one document.
	**  
	** @see `https://www.mongodb.com/docs/manual/reference/command/find/`
	[Str:Obj?]? findOne(Str:Obj? filter, Bool checked := true) {
		l := cmd("find", name)
			.add("filter",		filter)
			.add("batchSize",	2)
			.add("limit",		2)
			.cursor.toList
		if (l.size > 1)
			throw Err("findOne() returned multiple documents from ${qname} - ${filter}")
		if (l.isEmpty && checked)
			throw Err("findOne() returned ZERO documents from ${qname} - ${filter}")
		return l.first
	}

	** Returns documents that match the given filter. Many options are possible.
	** 
	** pre>
	** syntax: fantom
	** findMany(["rick":"morty"]) {
	**   it->sort        = ["fieldName":1]
	**   it->hint        = "_indexName_"
	**   it->skip        = 50
	**   it->limit       = 100
	**   it->projection  = ["_id":1, "name":1]
	**   it->batchSize   = 101
	**   it->singleBatch = true
	**   it->collation   = [...]
	** }.toList
	** <pre
	** 
	**  - @see `https://www.mongodb.com/docs/manual/reference/command/find/`
	**  - @see `https://www.mongodb.com/docs/manual/tutorial/query-documents/`
	MongoCur find([Str:Obj?]? filter := null, |MongoCmd cmd|? optsFn := null) {
		cmd("find", name)
			.add("filter",	filter)
			.withFn(		optsFn)
			.cursor
	}

	** Runs the given 'updateCmd' against documents that match the given filter.
	** 
	** pre>
	** syntax: fantom
	** update(["rick":"morty"], ["\$set":["rick":"sanchez"]]) {
	**   it->upsert      = true
	**   it->multi       = false  // defaults to true
	**   it->hint        = "_indexName_"
	**   it->collation   = [...]
	** }
	** <pre
	** 
	** Inspect return value for upserted IDs.
	** 
	** *(By default, this will update multiple documents.)*
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/update/`
	** @see `https://www.mongodb.com/docs/manual/reference/operator/update/`
	Str:Obj? update(Str:Obj? filter, Str:Obj? updates, |MongoCmd cmd|? optsFn := null) {
		updateCmd := cmd("q",	filter)
			.add("u",			updates)
			.withFn(			optsFn)
			.add("multi",		true)	// default to multi-doc updates
		opts := updateCmd.extract("ordered writeConcern bypassDocumentValidation comment let".split)
		return cmd("update",	 name)
			.add("updates",		[updateCmd])
			.addAll(			opts)
			.add("writeConcern",connMgr.writeConcern)
			.run
	}

	** Deletes documents that match the given filter, and returns the number of documents deleted.
	** 
	** pre>
	** syntax: fantom
	** delete(["rick":"morty"]) {
	**   it->limit     = 1
	**   it->hint      = "_indexName_"
	**   it->collation = [...]
	** }
	** <pre
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/delete/`
	Int delete(Str:Obj? filter, |MongoCmd cmd|? optsFn := null) {
		deleteCmd := cmd("q",	filter)
			.withFn(			optsFn)
		opts := deleteCmd.extract("comment let ordered writeConcern".split)
		return cmd("delete",	 name)
			.add("deletes",		[deleteCmd])
			.addAll(			opts)
			.add("writeConcern",connMgr.writeConcern)
			.run["n"]->toInt
	}

	** Deletes ALL documents in a Collection.
	** 
	** Note this is MUCH quicker than dropping the Collection.
	Int deleteAll() {
		delete([:]) { it->limit=0 }
	}	
	
	// TODO support findAndModify() commands
	// findAndModify() commands show no discernible advantage over separate
	// update() & find() commands, or find() followed by update().
	// findAndModify() is not atomic, and has the same semantics,
	// only it's much more confusing!
	//
	// So I'm voting to leave them out for now - even though it IS in the Stable API.
	
	** @see 
	**  - `http://docs.mongodb.org/manual/reference/command/aggregate/`
	**  - `http://docs.mongodb.org/manual/reference/aggregation/`
	MongoCur aggregate([Str:Obj?][] pipeline, |MongoCmd cmd|? optsFn := null) {
		cmd("aggregate",		name)
			.add("pipeline", 	pipeline)
			.withFn(			optsFn)
			.add("cursor",		Str:Obj?[:])	// MUST specify an empty cursor
			.add("writeConcern",connMgr.writeConcern)
			.cursor
	}
	
	** Returns the number of documents that match the given filter.
	Int count([Str:Obj?]? filter := null) {
		aggregate([
			[
				"\$match"			: filter ?: Str:Obj?[:]
			],
			[
				"\$group"			: Str:Obj?[
					"_id"			: null,
					"count"			: Str:Obj?[
						"\$sum"		: 1
					]
				]
			]
		]).toList.first["count"]
	}

	** Returns the number of documents in the collection.
	** 
	** The count is based on the collection's metadata, which provides a fast but sometimes 
	** inaccurate count for sharded clusters.
	Int size() {
		aggregate([
			[
				"\$collStats" 	: [
					"count"		: Str:Obj?[:]
				]
			]
		]).toList.first["count"]
	}
	
	Str qname() {
		"${dbName}.${name}"
	}
		
	internal static Str validateName(Str name) {
		if (name.isEmpty)
			throw ArgErr("Collection name can not be empty")
		if (name.any { it == '$' })
			throw ArgErr("Collection name '${name}' may not contain any of the following: \$")
		return name
	}
	
	@NoDoc
	override Str toStr() { qname }

	** **For Power Users!**
	** 
	** Don't forget to call 'run()'!
	private MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, dbName, cmdName, cmdVal)
	}
}
