
** Represents a MongoDB collection.
** 
** @see `https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst`
const class MongoColl {
	
	** The underlying connection manager.
	const MongoConnMgr connMgr
	
	** The name of the database.
	const Str dbName

	** The simple name of the collection.
	const Str name

	** Creates a 'Collection' with the given name under the database.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new make(MongoConnMgr connMgr, Str name, Str? dbName := null) {
		this.connMgr 	= connMgr
		this.dbName		= MongoDb.validateName(dbName ?: connMgr.database)
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
		
	** Returns an 'MongoIdx' of the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	MongoIdx index(Str idxName) { 
		MongoIdx(connMgr, idxName, name, dbName)
	}
	
	** Returns all the indexes in this collection.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listIndexes/`
	MongoCur listIndexes() {
		cmd("listIndexes", name).cursor
	}

	** Returns all the index names in this collection.
	Str[] listIndexNames() {
		listIndexes.toList.map |i->Str| { i["name"] }
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
	**   it->size   = 64 * 1024  // no of bytes
	**   it->max    = 14         // no of docs
	** }
	** <pre
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/create/`
	Void create(|MongoCmd cmd|? optsFn := null) {
		cmd("create", name)
			.withFn(				optsFn)
			.add("writeConcern",	connMgr.writeConcern)
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
			.withFn(				optsFn)
			.add("documents",		documents)
			.add("writeConcern",	connMgr.writeConcern)
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
	** find(["rick":"morty"]) {
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
			.withFn(		optsFn)
			.add("filter",	filter)
			.cursor
	}
	
	** Performs a text search on the collection. 
	** 
	** Text searching makes use of stemming and ignores language stop words.
	** Quotes may be used to search for exact phrases and prefixing a word with a hyphen-minus (-) negates it.
	** 
	** Results are automatically ordered by search relevance.
	**  
	** To use text searching, make sure the Collection has a text Index else MongoDB will throw an Err.
	** 
	**   col.textSearch("some text")
	** 
	** 'options' may include the following:
	** 
	**   table:
	**   Name                 Type  Desc
	**   ----                 ----  ----                                              
	**   $language            Bool  Determines the list of stop words for the search and the rules for the stemmer and tokenizer. See [Supported Text Search Languages]`https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages`. Specify 'none' for simple tokenization with no stop words and no stemming. Defaults to the language of the index.
	**   $caseSensitive       Bool  Enable or disable case sensitive searching. Defaults to 'false'.
	**   $diacriticSensitive  Int   Enable or disable diacritic sensitive searching. Defaults to 'false'.
	** 
	** Text searches may be mixed with regular filters. See 'MongoQ' for details.
	** 
	** @see `https://docs.mongodb.com/manual/reference/operator/query/text/`.
	MongoCur textSearch(Str search, [Str:Obj?]? opts := null) {
		filter := Str:Obj?["\$text": ["\$search": search].addAll(opts ?: Str:Obj?[:])]
		return cmd("find", name)
			.add("filter",		filter)
			.add("projection",	["_textScore": ["\$meta": "textScore"]])
			.add("sort",		["_textScore": ["\$meta": "textScore"]])
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
			.withFn(			optsFn)
			.add("u",			updates)
			.add("multi",		true)	// default to multi-doc updates
		opts := updateCmd.extract("ordered writeConcern bypassDocumentValidation comment let".split)
		return cmd("update",	 name)
			.add("updates",		[updateCmd.cmd])
			.addAll(			opts)
			.add("writeConcern",connMgr.writeConcern)
			.run
	}
	
	** Finds a single document that matches the given filter, and replaces it.
	** The '_id' field is NOT replaced.
	** 
	** pre>
	** syntax: fantom
	** replace(["rick":"morty"], ["rick":"sanchez"]) {
	**   it->upsert  = true
	** }
	** <pre
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/update/`
	** @see `https://www.mongodb.com/docs/manual/reference/operator/update/`
	Str:Obj? replace(Str:Obj? filter, Str:Obj? replacement, |MongoCmd cmd|? optsFn := null) {
		updateCmd := cmd("q",	filter)
			.withFn(			optsFn)
			.add("u",			replacement)
			.add("multi",		false)	// default to multi-doc updates
		opts := updateCmd.extract("ordered writeConcern bypassDocumentValidation comment let".split)
		return cmd("update",	 name)
			.add("updates",		[updateCmd.cmd])
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
			.add("limit",		0)		// 0 == delete all matching documents
		opts := deleteCmd.extract("comment let ordered writeConcern".split)
		return cmd("delete",	 name)
			.add("deletes",		[deleteCmd.cmd])
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
	
	** Finds, updates, and returns a single document.
	** Returns 'null' if no matching document was found.
	**  
	** @see `https://www.mongodb.com/docs/manual/reference/command/findAndModify/`
	[Str:Obj?]? findAndUpdate(Str:Obj? filter, Str:Obj? update, |MongoCmd cmd|? optsFn := null) {
		cmd("findAndModify",		name)
			.withFn(				optsFn)
			.add("query",			filter)
			.add("update",			update)
			.add("new",				true)
			.add("writeConcern",	connMgr.writeConcern)
			.run["value"]
	}

	** Finds, deletes, and returns a single document.
	** Returns 'null' if no matching document was found.
	**  
	** @see `https://www.mongodb.com/docs/manual/reference/command/findAndModify/`
	[Str:Obj?]? findAndDelete(Str:Obj? filter, |MongoCmd cmd|? optsFn := null) {
		cmd("findAndModify",		name)
			.withFn(				optsFn)
			.add("query",			filter)
			.add("remove",			true)
			.add("writeConcern",	connMgr.writeConcern)
			.run["value"]
	}
	
	** Processes documents through an aggregation pipeline.
	** 
	** @see 
	**  - `http://docs.mongodb.org/manual/reference/command/aggregate/`
	**  - `http://docs.mongodb.org/manual/reference/aggregation/`
	MongoCur aggregate([Str:Obj?][] pipeline, |MongoCmd cmd|? optsFn := null) {
		cmd("aggregate",		name)
			.withFn(			optsFn)
			.add("pipeline", 	pipeline)
			.add("cursor",		Str:Obj?[:])	// MUST specify an empty cursor
			.add("writeConcern",connMgr.writeConcern)
			.cursor
	}
	
	** Returns the number of documents that match the given filter.
	** (Uses an 'aggregate' cmd.)
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
		]).toList.first?.get("count") ?: 0
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
	
	** Returns the qualified name of this collection.
	** It takes the form of:
	**
	**   <database>.<collection>
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
