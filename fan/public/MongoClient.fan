using concurrent
using inet

** A MongoDB client.
** 
** This class is the main starting point for connecting to a MongoDB instance. 
** 
** Retrieving data from MongoDB can be as easy as:
** 
**   mongo := MongoClient(ActorPool(), "127.0.0.1", 27017)
**   data  := mongo.db("db").collection("col").findAll
** 
** Or using defaults and shorthand notation:
** 
**   mongo := MongoClient(ActorPool())
**   data  := mongo["db"]["col"].findAll
** 
const class MongoClient {
	private static const Log 		log	:= Utils.getLog(MongoClient#)
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
		Database(conMgr, dbName)
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
	
	private Void startup() {
		conMgr.startup
		
		minVersion	 := Version("2.6.0")
		buildVersion := buildInfo["version"]
		mongoVersion := Version.fromStr(buildVersion, false)
		log.info("\n" + logo + "\nConnected to MongoDB v${buildVersion} (at ${conMgr.mongoUrl})\n")

		if (mongoVersion < minVersion) {
			msg := "** WARNING: This driver is ONLY compatible with MongoDB v${minVersion} or greater **"
			log.warn(Str.defVal.padl(msg.size, '*'))
			log.warn(msg)
			log.warn(Str.defVal.padl(msg.size, '*'))
		}
	}
	
	private Str logo() {
		"
		      Alien-Factory     
		  _____ ___ ___ ___ ___ 
		 |     | . |   | . | . |
		 |_|_|_|___|_|_|_  |___|
		               |___|${Pod.of(this).version}
		 "
	}
}
