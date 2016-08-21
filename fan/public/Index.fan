
** Represents a MongoDB index.
const class Index {

	private const Namespace			colNs
	private const Namespace			idxNs
	private const ConnectionManager conMgr
	
	** Use in 'key' arguments to denote sort order.
	static const Int ASC	:= 1
	
	** Use in 'key' arguments to denote sort order.
	static const Int DESC	:= -1
	
	** The name of this index. 
	const Str	name
	
	** Creates an 'Index' with the given details.
	new make(ConnectionManager conMgr, Str collectionQname, Str indexName, |This|? f := null) {
		f?.call(this)
		this.conMgr	= conMgr
		this.colNs	= Namespace(collectionQname)
		this.idxNs	= colNs.withCollection("system.indexes")
		this.name	= indexName
	}

	internal new makeWithNs(ConnectionManager conMgr, Namespace namespace, Str indexName, |This|? f := null) {
		f?.call(this)
		this.conMgr	= conMgr
		this.colNs	= namespace
		this.idxNs	= colNs.withCollection("system.indexes")
		this.name	= indexName
	}

	** Returns index info.
	** 
	** Returns 'null' if index does not exist.
	** 
	** @see `http://docs.mongodb.org/manual/reference/method/db.collection.getIndexes/`
	[Str:Obj?]? info() {
		res := cmd.add("listIndexes", colNs.collectionName).run
		nfo := ([Str:Obj?][]) res["cursor"]->get("firstBatch")
		return nfo.find { it["name"] == name }
	}

	** *For use with MongoDB v2.6.x only*
	** 
	** Returns index info.
	** 
	** @see `http://docs.mongodb.org/manual/reference/method/db.collection.getIndexes/`
	@Deprecated { msg="For use with MongoDB v2.6.x only" }
	Str:Obj? info26() {
		Collection(conMgr, idxNs.qname).findOne(["ns":colNs.qname, "name":name])
	}
	
	** Returns 'true' if this index exists.
	Bool exists() {
		info != null	
	}

	** *For use with MongoDB v2.6.x only*
	** 
	** Returns 'true' if this index exists.
	@Deprecated { msg="For use with MongoDB v2.6.x only" }
	Bool exists26() {
		res  := cmd.add("listIndexes", colNs.collectionName).run
		info := res["cursor"]->get("firstBatch") 
		info{echo(it)}
		
		res["cursor"]->get("firstBatch")->isEmpty->not

		return Collection(conMgr, idxNs.qname).findCount(["ns":colNs.qname, "name":name]) > 0		
	}
	
	** Creates this index.
	** 
	** 'key' is a map of fields to index type. 
	** Values may either be the standard Mongo '1' and '-1' for ascending / descending or the 
	** strings 'ASC' / 'DESC'.
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
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	This create(Str:Obj key, Bool? unique := false, Str:Obj options := [:]) {
		if (key.size > 1 && key.ordered == false)
			throw ArgErr(ErrMsgs.cursor_mapNotOrdered(key))

		// there's no createIndexMulti 'cos I figure no novice will need to create multiple indexes at once!
		if (unique == true)	options.set("unique", unique)
		cmd	.add("createIndexes", colNs.collectionName)
			.add("indexes", 	[cmd
				.add("key",		Utils.convertAscDesc(key))
				.add("name",	name)
				.addAll(options)
				.query
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
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	Bool ensure(Str:Obj key, Bool? unique := false, Str:Obj options := [:]) {
		if (key.size > 1 && key.ordered == false)
			throw ArgErr(ErrMsgs.cursor_mapNotOrdered(key))

		if (!exists) {
			create(key, unique, options)
			return true
		}
		// if null or false, unique does not appear in the index info map
		if (unique == true)	options.set("unique", unique)
		
		info := info
		oldKeyMap := (Str:Obj?) info["key"]
		newKeyMap := Map.make(oldKeyMap.typeof).addAll(Utils.convertAscDesc(key))
		
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
		if (force || exists) cmd.add("dropIndexes", colNs.collectionName).add("index", name).run
		// [nIndexesWas:2, ok:1.0]
		return this
	}

	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, colNs, action)
	}	
	
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		"${idxNs.databaseName}::${name}"
	}

}
