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

	// ---- Database ------------------------------------------------------------------------------

	** Drops the database. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropDatabase/`
	This drop() {
		cmd.add("dropDatabase", 1).run
		// [dropped:afMongoTest, ok:1.0]
		return this
	}
	
	** **For Power Users!**
	** 
	** Runs any arbitrary command against this database.
	** 
	** Note you must set the write concern yourself, should the command take one. 
	[Str:Obj?] runCmd(Str:Obj? cmd) {
		this.cmd("cmd").addAll(cmd).run
	}
	
	// ---- Diagnostics  --------------------------------------------------------------------------
	
	** Returns storage statistics for this database.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dbStats/`
	Str:Obj? stats(Int scale := 1) {
		cmd.add("dbStats", 1).add("scale", scale).run
	}
	
	// ---- Collections ---------------------------------------------------------------------------
	
	** Returns 'true' if this collection exists.
	Str[] collectionNames() {
		res := cmd.add("listCollections", 1).run
		cur := (Str:Obj?)     res["cursor"]
		bat := ([Str:Obj?][]) cur["firstBatch"]
		return bat.map |nom->Str| { nom["name"] }
	}

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
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private MongoCmd cmd(Str? action := null) {
		MongoCmd(conMgr, Namespace(name, "wotever"), action)
	}
	
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		name
	}
}
