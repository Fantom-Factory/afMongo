
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
	
	internal new make(ConnectionManager conMgr, Namespace namespace, Str name) {
		this.conMgr	= conMgr
		this.colNs	= namespace
		this.idxNs	= Namespace(colNs.databaseName, "system.indexes")
		this.name	= name
	}

	** Returns index info.
	** 
	** @see `http://docs.mongodb.org/manual/reference/method/db.collection.getIndexes/#db.collection.getIndexes`
	Str:Obj? info() {
		Collection(conMgr, idxNs.qname).findOne(["ns":colNs.qname, "name":name])
	}
	
	** Returns 'true' if this index exists.
	Bool exists() {
		Collection(conMgr, idxNs.qname).findCount(["ns":colNs.qname, "name":name]) > 0		
	}
	
	** Creates this index.
	** 
	** 'key' is a map of fields to index type, use 1 for ascending and -1 for descending.
	** 
	** 'unique' if not specified, defaults to 'false'.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	This create(Str:Obj key, Bool? unique := null, Str:Obj options := [:]) {
		// there's no createIndexMulti 'cos I figure no novice will need to create multiple indexes at once!
		if (unique != null)	options.set("unique", unique)
		cmd	.add("createIndexes", colNs.collectionName)
			.add("indexes", 	[ [Str:Obj?][:] { it.ordered = true }
				.add("key",		key)
				.add("name",	name)
				.addAll(options)
			])
			.run
		// [createdCollectionAutomatically:false, numIndexesBefore:1, numIndexesAfter:2, ok:1.0]
		return this
	}
	
	** Ensures this index exists.
	** If the index does not exist, it is created. 
	** If it exists but with a different key / options, it is dropped and re-created.
	** 
	** 'unique' if not specified, defaults to 'false'.
	** 
	** Returns 'true' if the index was (re)-created, 'false' if nothing changed. 
	Bool ensure(Str:Obj key, Bool? unique := null, Str:Obj options := [:]) {
		if (!exists) {
			create(key, unique, options)
			return true
		}
		if (unique != null)	options.set("unique", unique)
		
		info := info
		oldKeyMap := (Str:Obj?) info["key"]
		newKeyMap := Map.make(oldKeyMap.typeof).addAll(key)
		
		if (info.size == options.size + 4 && oldKeyMap == newKeyMap && options.all |v, k| { info[k] == v })
			return false
		
		drop
		create(key, unique, options)
		return true
	}
	
	** Drops this index.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	This drop() {
		cmd.add("dropIndexes", colNs.collectionName).add("index", name).run
		// [nIndexesWas:2, ok:1.0]
		return this
	}

	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, colNs, action)
	}	
}
