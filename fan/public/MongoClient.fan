using concurrent
using inet

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
	
	@NoDoc	// I give no guarantee how long this field will stick around for!
	static const AtomicBool logBanner := AtomicBool(true)
	
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
	
	// ---- Diagnostics ---------------------------------------------------------------------------

	** Returns a list of existing databases with some basic statistics. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/listDatabases/`
	[Str:Obj?][] listDatabases() {
		runAdminCmd(["listDatabases": 1])["databases"]
	}

	** Returns a build summary
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/buildInfo/`
	[Str:Obj?] buildInfo() {
		runAdminCmd(["buildInfo": 1])
	}
	
	// ---- Database ------------------------------------------------------------------------------

	** Returns all the database names on the MongoDB instance. 
	Str[] listDatabaseNames() {
		listDatabases.map |db->Str| { db["name"] }.sort
	}

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

	// ---- Other ---------------------------------------------------------------------------------
	
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
	
	private Void startup() {
		conMgr.startup
		
		minVersion	 := Version("3.6.0")
		buildVersion := buildInfo["version"]
		mongoVersion := Version.fromStr(buildVersion, false)
		banner		 := logBanner.val ? "\n${logo}" : "" 
		log.info("${banner}\nConnected to MongoDB v${buildVersion} (at ${conMgr.mongoUrl})\n")

		if (mongoVersion < minVersion) {
			msg := "** WARNING: This driver is ONLY compatible with MongoDB v${minVersion} or greater **"
			log.warn(Str.defVal.padl(msg.size, '*'))
			log.warn(msg)
			log.warn(Str.defVal.padl(msg.size, '*'))
		}
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
