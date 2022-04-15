using concurrent::ActorPool

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
	private static const Log 		log	:= MongoClient#.pod.log
	private const ConnectionManager conMgr
	
	** Creates a 'MongoClient' with the given 'ConnectionManager'. 
	** This is the preferred ctor.
	new make(ConnectionManager connectionManager, |This|? f := null) {
		this.conMgr = connectionManager
		f?.call(this)
		startup()
	}
	
	** A convenience ctor - just to get you started!
	new makeFromUri(ActorPool actorPool, Uri mongoUri := `mongodb://localhost:27017`, |This|? f := null) {
		this.conMgr = ConnectionManagerPooled(actorPool, mongoUri)
		f?.call(this)
		startup()
	}
	
	// ---- Mongo Client ------------------------

	** Returns a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	Database db(Str dbName) {
		Database(conMgr, dbName)
	}

	** Convenience / shorthand notation for 'db(name)'
	@Operator
	Database get(Str dbName) {
		db(dbName)
	}
	
	// ---- Stable API --------------------------
	
	** Returns a list of existing databases with some basic statistics. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/listDatabases/`
	[Str:Obj?][] listDatabases() {
		runAdminCmd(["listDatabases" : 1])["databases"]
	}
	
	Str:Obj? ping() {
		runAdminCmd(["ping" : 1])
	}
	
	// TODO hello / isMaster

	// ---- Other -------------------------------

	** Returns a build summary
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/buildInfo/`
	[Str:Obj?] buildInfo() {
		runAdminCmd(["buildInfo": 1])
	}
	
	** Returns all the database names on the MongoDB instance. 
	Str[] listDatabaseNames() {
		listDatabases.map |db->Str| { db["name"] }.sort
	}

	** **For Power Users!**
	** 
	** Runs a command against the admin database. Convenience for:
	** 
	**   db("admin").runCmd(cmd)
	[Str:Obj?] runAdminCmd(Str:Obj? cmd) {
		db("admin").runCmd(cmd)
	}

	** Convenience for 'connectionManager.shutdown'.
	Void shutdown() {
		conMgr.shutdown
	}
	
	// ---- Private -----------------------------
	
	private Void startup() {
		conMgr.startup
		
		buildVersion := buildInfo["version"]
		banner		 := "\n${logo}\nConnected to MongoDB v${buildVersion} (at ${conMgr.mongoUrl})\n"
		log.info(banner)
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
