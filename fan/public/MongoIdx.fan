
** Represents a MongoDB index.
const class MongoIdx {
	
	** Use in 'key' arguments to denote field sort order.
	static const Int ASC	:= 1
	
	** Use in 'key' arguments to denote field sort order.
	static const Int DESC	:= -1
	
	** Use in 'key' arguments to denote a text index on the field.
	static const Str TEXT	:= "text"

	** The underlying connection manager.
	const MongoConnMgr connMgr
	
	** The name of the database.
	const Str	dbName
	
	** The simple name of the collection.
	const Str	collName
	
	** The simple name of this index. 
	const Str	name
	
	new make(MongoConnMgr connMgr, Str idxName, Str colName, Str? dbName := null) {
		this.connMgr	= connMgr
		this.dbName		= MongoDb.validateName(dbName ?: connMgr.database)
		this.collName	= MongoColl.validateName(colName)
		this.name		= idxName
	}
	
	** Creates an 'MongoDb' instance of the associated DB.
	MongoDb db() {
		MongoDb(connMgr, dbName)
	}
	
	** Creates a 'MongoColl' instance of the associated Collection.
	MongoColl coll() {
		MongoColl(connMgr, collName, dbName)
	}

	** Returns index info.
	** 
	** Returns 'null' if index does not exist.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listIndexes/`
	[Str:Obj?]? info() {
		cmd("listIndexes", collName)
			.cursor
			.toList
			.find { it["name"] == name }
	}
	
	** Returns 'true' if this index exists.
	Bool exists() {
		info != null	
	}
	
	** Creates this index.
	** 
	** 'key' is a map of fields to index type. 
	** If it contains more than 1 entry, it must be ordered.
	** 
	** Values should be the standard Mongo '1' and '-1' for ascending / descending, 
	** or the string 'TEXT'.
	** 
	** pre>
	** syntax: fantom
  	** index.create([
  	**   "dateAdded" : MongoIdx.DESC,
  	**   "name"      : MongoIdx.ASC,
  	** ], false) {
  	**   it->background         = true   // build in background
  	**   it->expireAfterSeconds = 60     // time in secs
  	** }
  	** <pre
  	** 
  	** Text indexes need to specify weights for each field.
	** 
	** pre>
	** syntax: fantom
  	** index.create([
  	**   "boringText"         : MongoIdx.TEXT,
  	**   "importantText"      : MongoIdx.TEXT,
  	** ], false) {
  	**   it->default_language = "english",  // optional
  	**   it->collation        = [...],      // optional 
  	**   it->weights          = [
  	**     "boringText"       : 5,
  	**     "importantText"    : 15          // is x3 more important than boring!
  	**   ]  
  	** }
  	** <pre
  	** 
  	** Note that Text Indexes are NOT part of the Mongo Stable API.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/createIndexes/`
	** @see `https://www.mongodb.com/docs/manual/core/index-text/`
	Str:Obj? create(Str:Obj key, Bool unique := false, |MongoCmd cmd|? optsFn := null) {
		if (key.size > 1 && key.ordered == false)
			throw ArgErr("Maps with more than 1 entry must be ordered: ${key}")
		createCmd := cmd("name",	name)
			.withFn(				optsFn)
			.add("key",				key)
			.add("unique",			unique)
		opts := createCmd.extract("writeConcern commitQuorum comment".split)
		return cmd("createIndexes",	collName)
			.add("indexes",			[createCmd.cmd])
			.addAll(				opts)
			.add("writeConcern",	connMgr.writeConcern)
			.run
	}
	
	** Ensures this index exists.
	** Returns 'true' if the index was (re)-created, 'false' if nothing changed.
	** 
	** If the index does not exist, it is created. 
	** 
	** If does exist, but with a different key, it is dropped and re-created. (Options are not checked.)
	Bool ensure(Str:Obj key, Bool unique := false, |MongoCmd cmd|? optsFn := null) {
		if (key.size > 1 && key.ordered == false)
			throw ArgErr("Maps with more than 1 entry must be ordered: ${key}")

		if (!exists) {
			create(key, unique, optsFn)
			return true
		}

		info	:= this.info
		oldUni	:= info["unique"] ?: false
		oldKey	:= info["key"] as Str:Obj?
		newKey	:= Map.make(oldKey.typeof).addAll(key)

		// some options just concern building the index, others are just not returned, 
		// so they're impossible to compare ... so lets just not try!
		if (optsFn == null && oldUni == unique && oldKey == newKey) {
			return false
		}

		drop
		create(key, unique, optsFn)
		return true
	}
	
	** Drops this index, but only if it exists.
	** 
	** If 'force' is 'true' then the index is dropped regardless. 
	** Note this may result in an error if the index does not exist.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/dropIndexes/`
	Void drop(Bool force := false) {
		if (force || exists) cmd("dropIndexes", collName).add("index", name).run
	}

	** **For Power Users!**
	** 
	** Don't forget to call 'run()' on the returned command!
	private MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, dbName, cmdName, cmdVal)
	}
	
	@NoDoc
	override Str toStr() {
		"${dbName}.${collName}::${name}"
	}
}
