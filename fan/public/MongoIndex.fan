
** Represents a MongoDB index.
const class MongoIndex {

	private const MongoConnMgr connMgr
	
	** Use in 'key' arguments to denote field sort order.
	static const Int ASC	:= 1
	
	** Use in 'key' arguments to denote field sort order.
	static const Int DESC	:= -1
	
	** Use in 'key' arguments to denote a text index on the field.
	static const Str TEXT	:= "text"
	
	const Str	dbName
	
	const Str	colName
	
	** The name of this index. 
	const Str	name
	
	new make(MongoConnMgr connMgr, Str dbName, Str colName, Str indexName) {
		this.connMgr	= connMgr
		this.dbName		= MongoDb.validateName(dbName)
		this.colName	= MongoCol.validateName(colName)
		this.name		= indexName
	}

	** Returns index info.
	** 
	** Returns 'null' if index does not exist.
	** 
	** @see `http://docs.mongodb.org/manual/reference/method/db.collection.getIndexes/`
	[Str:Obj?]? info() {
		res := cmd("listIndexes", colName).run
		// FIXME cursor
		nfo := ([Str:Obj?][]) res["cursor"]->get("firstBatch")
		return nfo.find { it["name"] == name }
	}
	
	** Returns 'true' if this index exists.
	Bool exists() {
		info != null	
	}
	
	** Creates this index.
	** 
	** 'key' is a map of fields to index type. 
	** Values may either be the standard Mongo '1' and '-1' for ascending / descending, the 
	** strings 'ASC' / 'DESC', or the string 'TEXT'.
	** 
	**   syntax: fantom
  	**   index.create(["dateAdded" : Index.ASC])
	** 
	** Note that should 'key' contain more than 1 entry, it must be ordered.
	** 
	** The 'options' parameter is merged with the Mongo command.
	** Options are numerous (see the MongoDB documentation for details) but common options are:
	** 
	**   table:
	**   Name               Type  Desc
	**   ----               ----  ----                                              
	**   background         Bool  Builds the index in the background so it does not block other database activities. Defaults to 'false'.
	**   sparse             Bool  Only reference documents with the specified field. Uses less space but behave differently in sorts. Defaults to 'false'.
	**   expireAfterSeconds Int   Specifies a Time To Live (TTL) in seconds that controls how long documents are retained.
	**   weights            Map   Specifies the relative weights of fields to use in text searching. See [Control Search Results with Weights]`https://docs.mongodb.com/manual/tutorial/control-results-of-text-search/`.
	**   default_language   Str   The language used to create text indexes. See [Supported Text Search Languages]`https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages`. Defaults to 'english'. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	This create(Str:Obj key, Bool? unique := false, Str:Obj options := [:]) {
		if (key.size > 1 && key.ordered == false)
			throw ArgErr(cursor_mapNotOrdered(key))

		// there's no createIndexMulti 'cos I figure no novice will need to create multiple indexes at once!
		if (unique == true)	options.set("unique", unique)
		cmd("createIndexes", colName)
			.add("indexes", 	[
				 cmd("key",		convertAscDesc(key))
				.add("name",	name)
				.addAll(options)
				.cmd
			])
			.run
		// [createdCollectionAutomatically:false, numIndexesBefore:1, numIndexesAfter:2, ok:1.0]
		return this
	}
	
	** Ensures this index exists.
	** If the index does not exist, it is created. 
	** If it exists but with a different key / options, it is dropped and re-created.
	** 
	** Returns 'true' if the index was (re)-created, 'false' if nothing changed.
	**
	**   syntax: fantom
	** 
	**   index.ensure(["dateAdded" : Index.ASC])
	**  
 	** Note that should 'key' contain more than 1 entry, it must be ordered.
	** 
	** The 'options' parameter is merged with the Mongo command.
	** Options are numerous (see the MongoDB documentation for details) but common options are:
	**  
	**   table:
	**   Name               Type  Desc
	**   ----               ----  ----                                              
	**   background         Bool  Builds the index in the background so it does not block other database activities.
	**   sparse             Bool  Only reference documents with the specified field. Uses less space but behave differently in sorts.
	**   expireAfterSeconds Int   Specifies a Time To Live (TTL) in seconds that controls how long documents are retained.
	**   weights            Map   Specifies the relative weights of fields to use in text searching. See [Control Search Results with Weights]`https://docs.mongodb.com/manual/tutorial/control-results-of-text-search/`.
	**   default_language   Str   The language used to create text indexes. See [Text Search Languages]`https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages` for a list of valid options. Defaults to 'english'. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	Bool ensure(Str:Obj key, Bool? unique := false, Str:Obj options := [:]) {
		if (key.size > 1 && key.ordered == false)
			throw ArgErr(cursor_mapNotOrdered(key))

		if (!exists) {
			create(key, unique, options)
			return true
		}
		// if null or false, unique does not appear in the index info map
		if (unique == true)	options.set("unique", unique)
		
		info := info
		oldKeyMap := (Str:Obj?) info["key"]
		newKeyMap := Map.make(oldKeyMap.typeof).addAll(convertAscDesc(key))
		
		if (info.size == options.size + 4 && oldKeyMap == newKeyMap && options.all |v, k| { info[k] == v })
			return false
		
		drop
		create(key, unique, options)
		return true
	}
	
	** Drops this index, but only if it exists.
	** 
	** If 'force' is 'true' then the index is dropped regardless. 
	** Note this may result in an error if the index doesn't exist.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	This drop(Bool force := false) {
		if (force || exists) cmd("dropIndexes", colName).add("index", name).run
		// [nIndexesWas:2, ok:1.0]
		return this
	}

	// ---- Private Methods -----------------------------------------------------------------------
	
	** **For Power Users!**
	** 
	** Don't forget to call 'run()'!
	private MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, name, cmdName)
	}
	
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		"${dbName}.${colName}::${name}"
	}

	private static Str cursor_mapNotOrdered(Str:Obj? map) {
		"Maps with more than 1 entry must be ordered: ${map}"
	}
	
	private static const Str[]	ascSynonymns	:= "asc       ascending  up  north heaven wibble".lower.split
	private static const Str[]	dscSynonymns	:= "dsc desc descending down south  hell  wobble".lower.split

	internal static [Str:Obj?] convertAscDesc(Str:Obj? doc) {
		doc.map |v| { 
			if (v isnot Str) return v
			if (ascSynonymns.contains((v as Str).lower)) return ASC
			if (dscSynonymns.contains((v as Str).lower)) return DESC
			return v
		}
	}
}
