
** A MongoDB client.
** 
** This class is the main starting point for connecting to a MongoDB instance. 
** 
** Retrieving data from a MongoDB can be as easy as:
** 
**   syntax: fantom
** 
**   mongo := MongoClient(`mongodb://localhost:27017/`)
**   data  := mongo.db("db").collection("col").findMany
** 
** Or using defaults and shorthand notation:
** 
**   syntax: fantom
** 
**   data  := mongo["db"]["col"].findMany
** 
const class MongoClient {

	** The connection manager that Mongo connections are leased from.
	const MongoConnMgr	connMgr
	
	** Creates a 'MongoClient' with the given 'ConnectionManager'. 
	new make(MongoConnMgr connMgr) {
		this.connMgr = connMgr
		startup()
	}
	
	** Creates a 'MongoClient' with a pooled connection to the given Mongo connection URL. 
	new makeFromUri(Uri mongoUrl) {
		this.connMgr = MongoConnMgrPool(mongoUrl)
		startup()
	}
	
	** Returns a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	MongoDb db(Str dbName) {
		MongoDb(connMgr, dbName)
	}

	** Convenience / shorthand notation for 'db(name)'
	@Operator
	MongoDb get(Str dbName) {
		db(dbName)
	}

	** **For Power Users!**
	** 
	** Runs an arbitrary command against the 'admin' database.
	** 
	** Don't forget to call 'run()'!
	MongoCmd adminCmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, "admin", cmdName, cmdVal)
	}

	** Convenience for 'MongoConnMgr.shutdown()'.
	Void shutdown() {
		connMgr.shutdown
	}
	
	
	
	// ---- Commands ----------------------------
	
	** Returns a build information of the connected MongoDB server.
	** Use to obtain the version of the MongoDB server.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/buildInfo/`
	Str:Obj? buildInfo() {
		adminCmd("buildInfo").run
	}
	
	** Sends a 'hello' command - if 'hello' is not available, a legacy 'isMaster' command is sent 
	** instead.
	Str:Obj? hello() {
		doc := adminCmd("hello").run(false)
		if (doc["ok"] != 1f)
			doc = adminCmd("isMaster").run(true)
		return doc
	}
	
	** Returns a list of existing databases, 
	** along with some basic info. 
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/listDatabases/`
	[Str:Obj?][] listDatabases([Str:Obj?]? filter := null) {
		adminCmd("listDatabases")
			.add("filter", filter)
			.run
			.get("databases")
	}
	
	** Returns all the database names on the MongoDB instance. 
	** 
	** This is more optimised than just calling 'listDatabases()'.
	Str[] listDatabaseNames() {
		((adminCmd("listDatabases")
			.add("nameOnly", true)
			.run
			.get("databases")) as [Str:Obj?][])
			.map |i->Str| { i["name"] }.sort
	}
	
	** Sends a 'ping' command to the server. 
	** 'pings' should return straight away, even if the server is write-locked. 
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/command/ping/`
	Str:Obj? ping() {
		adminCmd("ping").run
	}
	
	
	
	// ---- Helpers -----------------------------
	
	private Void startup() {
		connMgr.startup
		
		buildVersion := buildInfo["version"]
		banner		 := "\n${logo}\nConnected to MongoDB v${buildVersion} (at ${connMgr.mongoUrl})\n"
		connMgr.log.info(banner)
	}
	
	private Str logo() {
		"
		      Fantom-Factory     
		  _____ ___ ___ ___ ___ 
		 |     | . |   | . | . |
		 |_|_|_|___|_|_|_  |___|
		               |___|${Pod.of(this).version}
		 "
	}
}
