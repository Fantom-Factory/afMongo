//using afBson::Code

** Represents a MongoDB database.
const class Database {

	** The underlying connection manager.
	const ConnectionManager conMgr
	
	** The name of the database.
	const Str name

	** Creates a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new makeWithName(ConnectionManager connectionManager, Str name, |This|? f := null) {
		f?.call(this)
		this.conMgr = connectionManager
		this.name = Namespace.validateDatabaseName(name)
	}

	// ---- Database ----------------------------

	** Returns a 'Collection' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	Collection collection(Str collectionName) {
		Collection(this, collectionName)
	}

	** Convenience / shorthand notation for 'collection(name)'
	@Operator
	Collection get(Str collectionName) {
		collection(collectionName)
	}

	// ---- Stable API --------------------------
	
	** Drops the database. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropDatabase/`
	This drop() {
		cmd.add("dropDatabase", 1).run
		// [dropped:afMongoTest, ok:1.0]
		return this
	}
	
	// ---- Other -------------------------------
	
	** Returns 'true' if this collection exists.
	Str[] collectionNames() {
		res := cmd.add("listCollections", 1).run
		cur := (Str:Obj?)     res["cursor"]
		bat := ([Str:Obj?][]) cur["firstBatch"]
		return bat.map |nom->Str| { nom["name"] }
	}
	
	** **For Power Users!**
	** 
	** Runs any arbitrary command against this database.
	** 
	** Note you must set the write concern yourself, should the command take one. 
	[Str:Obj?] runCmd(Str:Obj? cmd) {
		this.cmd.addAll(cmd).run
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private MongoCmd cmd() {
		MongoCmd(conMgr, name)
	}
	
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		name
	}
}
