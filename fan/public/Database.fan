using afMongo::ConnectionManager as MongoConnMgr
using afMongo::Collection as MongoCollection

** Represents a MongoDB database.
const class Database {
	
	** The underlying connection manager.
	const MongoConnMgr connMgr
	
	** The name of the database.
	const Str name

	** Creates a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in MongoDB. 
	new makeWithName(MongoConnMgr connMgr, Str name) {
		this.connMgr	= connMgr
		this.name		= validateName(name)
	}


	
	// ---- Database ----------------------------

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

	
	
	// ---- Stable API --------------------------
	
	** Drops the database. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropDatabase/`
	This drop() {
		cmd("dropDatabase").run
		// [dropped:afMongoTest, ok:1.0]
		return this
	}
	
	
	
	// ---- Other -------------------------------
	
	** Returns 'true' if this collection exists.
	Str[] collectionNames() {
		res := cmd("listCollections").run
		cur := (Str:Obj?)     res["cursor"]
		bat := ([Str:Obj?][]) cur["firstBatch"]
		return bat.map |nom->Str| { nom["name"] }
	}
	
	** **For Power Users!**
	** 
	** Runs an arbitrary command against this database.
	** 
	** Don't forget to call 'run()'!
	MongoCmd cmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, name, cmdName)
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
