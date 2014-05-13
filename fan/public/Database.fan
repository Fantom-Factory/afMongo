
** Represents a MongoDB database.
const class Database {
	internal const ConnectionManager conMgr
	
	** The name of the database.
	const Str name

	** Creates a 'Database' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	new makeWithName(ConnectionManager connectionManager, Str name) {
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
	
	** Runs the given command against this database.
	[Str:Obj?] runCmd(Str:Obj? cmd) {
		// don't pass in a writeConcern, leave it up to the user
		this.cmd("cmd").addAll(cmd).run
	}

	** Evaluates a JavaScript function on the database server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/eval/`
	Obj? eval(Str func, Obj?[] args := [,], Bool noLock := false) {
		cmd	.add("eval",	func)
			.add("args", 	args)
			.add("nolock", 	noLock)
			.run["retval"]
	}

	** Executes the given function passing in a database (connection) that has been authenticated 
	** with the given user. Within the function, the authenticated database may be used as often 
	** as you wish.   
	** 
	**   wibble := db.authenticate("ZeroCool", "password") |authDb -> Obj?| {
	** 
	**       return authDb["collection"].findOne(["wibble": true])
	**   }
	** 
	** All Mongo objects ( 'Collection', 'Index', 'User', etc...) created from the authenticated
	** database will inherit the user credentials.
	Obj? authenticate(Str userName, Str password, |Database db->Obj?| func) {
		nonce 	:= (Str) cmd.add("getnonce", 1).run["nonce"]
		passdig	:= "${userName}:mongo:${password}".toBuf.toDigest("MD5").toHex
		digest	:=  ( nonce + userName + passdig ).toBuf.toDigest("MD5").toHex
		
		return conMgr.leaseConnection |connection->Obj?| {
			cmd := Str:Obj?[:] { ordered = true }
				.add("authenticate", 1)
				.add("user", 		 userName)
				.add("nonce", 		 nonce)
				.add("key", 		 digest)
			
			Operation(connection).runCommand("${name}.\$cmd", cmd)			
	
			try {
				cm := ConnectionManagerSingleThread(connection)
				db := Database(cm, name)
				return func.call(db)
			
			} finally {
				Operation(connection).runCommand("${name}.\$cmd", ["logout": 1])
			}
		}
	}
	
	// ---- Diagnostics  --------------------------------------------------------------------------
	
	** Returns storage statistics for this database.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dbStats/`
	Str:Obj? stats(Int scale := 1) {
		cmd.add("dbStats", 1).add("scale", scale).run
	}
	
	// ---- Collections ---------------------------------------------------------------------------
	
	** Returns all the collection names in the database. 
	Str[] collectionNames() {
		// if it wasn't for F4, I could have this all on one line!
		docs  := collection("system.namespaces").findAll
		names := (Str[]) docs.map |ns->Str| { ns["name"] }
		return names.exclude { !it.startsWith(name) || it.contains("\$") || it.contains(".system.") }.map { it[(name.size+1)..-1] }.sort
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

	// ---- Users ---------------------------------------------------------------------------------
	
	** Returns all the index names of this collection.
	Str[] userNames() {
		users := ([Str:Obj?][]) cmd.add("usersInfo", 1).run["users"]
		return users.map |user->Str| { user["user"] }.sort
	}
	
	** Returns a 'User' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	User user(Str userName) {
		User(conMgr, name, userName)
	}
	
	** Drops ALL users from this database. *Be careful!*
	** 
	** Returns the number of users dropped.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/dropAllUsersFromDatabase/`
	Int dropAllUsers() {
		cmd.add("dropAllUsersFromDatabase", 1).run["n"]->toInt
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, Namespace(name, "wotever"), action)
	}
}
