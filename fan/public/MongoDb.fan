using afMongo::Collection as MongoCollection

** Represents a MongoDB database.
const class MongoDb {
	
	** The connection manager that Mongo connections are leased from.
	const MongoConnMgr	connMgr
	
	** The name of the database.
	const Str name

	** Creates a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	new makeWithName(MongoConnMgr connMgr, Str name) {
		this.connMgr	= connMgr
		this.name		= validateName(name)
	}

	** Returns a 'Collection' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	MongoCollection collection(Str collectionName) {
		MongoCollection(connMgr, name, collectionName)
	}

	** Convenience / shorthand notation for 'collection(name)'
	@Operator
	MongoCollection get(Str collectionName) {
		collection(collectionName)
	}
	
	** **For Power Users!**
	** 
	** Runs an arbitrary command against this database.
	** 
	** Don't forget to call 'run()'!
	MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, name, cmdName)
	}

	
	
	// ---- Commands ----------------------------
	
	** Drops the database. * **Be careful!** *
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/dropDatabase/`
	Str:Obj? drop() {
		cmd("dropDatabase")
			.add("writeConcern",	connMgr.writeConcern)
			.run
	}
	
	** Returns a list of collections and views in this database,
	** along with some basic info. 
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listCollections/`
	Str:Obj? listCollections([Str:Obj?]? filter := null) {
		cmd("listCollections")
			.add("filter", filter)
			.run
		// FIXME cursor!
//			.get("databases"
//		cur := (Str:Obj?)     res["cursor"]
//		bat := ([Str:Obj?][]) cur["firstBatch"]
//		return bat.map |nom->Str| { nom["name"] }
	}
	
	** Returns 'true' if this collection exists.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listCollections/`
	Str[] collectionNames() {
		res := cmd("listCollections").add("nameOnly", true).run
		// FIXME cursor!
		cur := (Str:Obj?)     res["cursor"]
		bat := ([Str:Obj?][]) cur["firstBatch"]
		return bat.map |nom->Str| { nom["name"] }
	}
	
	
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
