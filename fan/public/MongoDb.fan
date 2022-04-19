
** Represents a MongoDB database.
const class MongoDb {
	
	** The underlying connection manager.
	const MongoConnMgr	connMgr
	
	** The name of the database.
	const Str name

	** Creates a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	new make(MongoConnMgr connMgr, Str name) {
		this.connMgr	= connMgr
		this.name		= validateName(name)
	}

	** Returns a 'Collection' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	MongoColl collection(Str collectionName) {
		MongoColl(connMgr, name, collectionName)
	}

	** Convenience / shorthand notation for 'collection(name)'
	@Operator
	MongoColl get(Str collectionName) {
		collection(collectionName)
	}
	
	** **For Power Users!**
	** 
	** Runs an arbitrary command against this database.
	** 
	** Don't forget to call 'run()'!
	MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, name, cmdName, cmdVal)
	}

	
	
	// ---- Commands ----------------------------
	
	** Drops the database. * **Be careful!** *
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/dropDatabase/`
	Str:Obj? drop() {
		cmd("dropDatabase")
			.add("writeConcern", connMgr.writeConcern)
			.run
	}
	
	** Returns a list of collections and views in this database,
	** along with some basic info. 
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listCollections/`
	MongoCur listCollections([Str:Obj?]? filter := null) {
		cmd("listCollections")
			.add("filter", filter)
			.cursor
	}
	
	** Returns all the collection (and view) names in this Mongo database. 
	** 
	** This is more optimised than just calling 'listCollections()'.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listCollections/`
	Str[] listCollectionNames() {
		cmd("listCollections")
			.add("nameOnly", true)
			.cursor
			.toList
			.map { it["name"] }
	}	
	
	// TODO support authenticate() and the  x.509 authentication mechanism (Stable API)
	
	** See `https://www.mongodb.com/docs/manual/reference/limits/#naming-restrictions`
	private static const Int[] invalidNameChars	:= "/\\. \"*<>:|?".chars	

	internal static Str validateName(Str name) {
		if (name.isEmpty)
			throw ArgErr("Database name can not be empty")
		if (name.any { invalidNameChars.contains(it) })
			throw ArgErr("Database name '${name}' may not contain any of the following: ${Str.fromChars(invalidNameChars)}")
		return name
	}
	
	@NoDoc
	override Str toStr() { name }
}
