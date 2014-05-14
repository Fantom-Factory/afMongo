using concurrent
using inet

** A MongoDB client.
const class MongoClient {
	
	private const ConnectionManager conMgr

	** This [write concern]`http://docs.mongodb.org/manual/reference/write-concern/` is passed down 
	** to all 'Database' instances created from this client.
	const [Str:Obj?] writeConcern	:= MongoConstants.defaultWriteConcern
	
	** Creates a 'MongoClient' with the given 'ConnectionManager'. 
	** This is the preferred ctor.
	new make(ConnectionManager connectionManager, |This|? f := null) {
		f?.call(this)
		this.conMgr = connectionManager
	}
	
	** A convenience ctor - just to get you started!
	new makeFromIpAddr(Str address := "127.0.0.1", Int port := 27017, |This|? f := null) {
		f?.call(this)
		this.conMgr = ConnectionManagerPooled(ActorPool(), IpAddr(address), port)
	}
	
	// ---- Diagnostics ---------------------------------------------------------------------------

	** Returns a list of existing databases with some basic statistics. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/listDatabases/`
	[Str:Obj?][] listDatabases() {
		runAdminCmd(["listDatabases": 1])["databases"]
	}

	** Returns a list of database commands. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/listCommands/`
	[Str:Obj?] listCommands() {
		runAdminCmd(["listCommands": 1])["commands"]
	}

	** Returns a build summary
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/buildInfo/`
	[Str:Obj?] buildInfo() {
		runAdminCmd(["buildInfo": 1])
	}

	** Returns info about the underlying MongoDB system.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/hostInfo/`
	[Str:Obj?] hostInfo() {
		runAdminCmd(["hostInfo": 1])
	}	

	** Returns an overview of the database process’s state.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/serverStatus/`
	[Str:Obj?] serverStatus() {
		runAdminCmd(["serverStatus": 1])
	}	
	
	// ---- Database ------------------------------------------------------------------------------

	** Returns all the database names on the MongoDB instance. 
	Str[] databaseNames() {
		listDatabases.map |db->Str| { db["name"] }.sort
	}

	** Returns a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	Database db(Str dbName) {
		Database(conMgr, dbName) { it.writeConcern = this.writeConcern }
	}

	** Convenience / shorthand notation for 'db(name)'
	@Operator
	Database get(Str dbName) {
		db(dbName)
	}

	// ---- Other ---------------------------------------------------------------------------------
	
	** Runs a command against the admin database.
	[Str:Obj?] runAdminCmd(Str:Obj? cmd) {
		db("admin").runCmd(cmd)
	}

	** Convenience for 'connectionManager.shutdown'.
	Void shutdown() {
		conMgr.shutdown
	}
}
