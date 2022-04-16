using concurrent::ActorPool
using afMongo::ConnectionManager as MongoConnMgr
using afMongo::ConnectionManagerPooled as MongoConnMgrPool
using afMongo::Database as MongoDb

** A MongoDB client.
** 
** This class is the main starting point for connecting to a MongoDB instance. 
** 
** Retrieving data from a MongoDB can be as easy as:
** 
**   syntax: fantom
** 
**   mongo := MongoClient(ActorPool(), `mongodb://localhost:27017`)
**   data  := mongo.db("db").collection("col").findAll
** 
** Or using defaults and shorthand notation:
** 
**   syntax: fantom
** 
**   mongo := MongoClient(ActorPool())
**   data  := mongo["db"]["col"].findAll
** 
const class MongoClient {
	** The connection manager that Mongo connections are leased from.
	const MongoConnMgr	connMgr
	
	** Creates a 'MongoClient' with the given 'ConnectionManager'. 
	new make(MongoConnMgr connMgr) {
		this.connMgr = connMgr
		startup()
	}
	
	** A convenience ctor - just to get you started!
	new makeFromUri(Uri mongoUri := `mongodb://localhost:27017`) {
		this.connMgr = MongoConnMgrPool(ActorPool(), mongoUri, null)
		startup()
	}
	
	// ---- Mongo Client ------------------------

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
	
	// ---- Stable API --------------------------
	
	** Returns a list of existing databases with some basic statistics. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/listDatabases/`
	[Str:Obj?][] listDatabases() {
		adminCmd("listDatabases")["databases"]
	}
	
	Str:Obj? ping() {
		adminCmd("ping").run
	}
	
	// TODO hello / isMaster

	// ---- Other -------------------------------

	** Returns a build information of the connected MongoDB server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/buildInfo/`
	Str:Obj? buildInfo() {
		adminCmd("buildInfo").run
	}
	
	** Returns all the database names on the MongoDB instance. 
	Str[] listDatabaseNames() {
		listDatabases.map |db->Str| { db["name"] }.sort
	}

	** **For Power Users!**
	** 
	** Runs an arbitrary command against the 'admin' database.
	** 
	** Don't forget to call 'run()'!
	MongoCmd adminCmd(Str cmdName, Obj? cmdVal := 1) {
		MongoCmd(connMgr, "admin", cmdName)
	}

	** Convenience for 'MongoConnMgr.shutdown()'.
	Void shutdown() {
		connMgr.shutdown
	}
	
	// ---- Private -----------------------------
	
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
