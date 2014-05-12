
** Represents a MongoDB index.
const class Index {

	private const Namespace			colNs
	private const Namespace			idxNs
	private const ConnectionManager conMgr
	
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
		Collection(conMgr, idxNs.qname).findOne(["ns":colNs.qname, "name":name], false) != null		
	}
	
	** Drops this index.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	This drop() {
		cmd("drop").add("dropIndexes", colNs.collectionName).add("index", name).run
		return this
	}
	
	** Creates this index.
	** 
	** 'key' is a map of fields to index type, use 1 for ascending and -1 for descending.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	This create(Str:Obj key, Bool unique := false, Str:Obj options := [:]) {
		// there's no createIndexMulti 'cos I figure no novice will need to create multiple indexes at once!
		cmd("insert")
			.add("createIndexes", colNs.collectionName)
			.add("indexes", 	[ [Str:Obj?][:] { it.ordered = true }
				.add("key",		key)
				.add("name",	name)
				.addAll(options.set("unique", unique))
			])
			.run
		return this
	}
	
	** Ensures this index exists.
	** If the index does not exist, it is created. 
	** If it exists but with a different key / options, it is dropped and re-created.
	** 
	** Returns 'true' if the index was (re)-created, 'false' if nothing changed. 
	Bool ensure(Str:Obj key, Bool unique := false, Str:Obj options := [:]) {
		if (!exists) {
			create(key, unique, options)
			return true
		}
		options.set("unique", unique)
		
		info := info
		oldKeyMap := (Str:Obj?) info["key"]
		newKeyMap := Map.make(oldKeyMap.typeof).addAll(key)
		
		if (info.size == options.size + 4 && oldKeyMap == newKeyMap && options.all |v, k| { info[k] == v })
			return false
		
		drop
		create(key, unique, options)
		return true
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str action, Bool checkForErrs := true) {
		Cmd(conMgr, colNs.databaseName, action, checkForErrs)
	}	
}
