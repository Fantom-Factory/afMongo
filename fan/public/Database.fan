using afBson

** Represents a MongoDB database.
const class Database {
	internal const ConnectionManager conMgr
	
	** The name of the database.
	const Str name

	** This [write concern]`http://docs.mongodb.org/manual/reference/write-concern/` is passed down 
	** to all 'Collection' and 'User' instances created by this 'Database'.
	const [Str:Obj?] writeConcern	:= MongoConstants.defaultWriteConcern

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

	** Evaluates a JavaScript function on the database server.
	**  
	**   scope := ["y":2]
	**   func  := Code("function (x) { return x + y; }", scope)
	**   xy    := db.eval(func, [3f])  // --> 5.0
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/eval/`
	Obj? eval(Code func, Obj?[] args := [,], Bool noLock := false) {
		cmd	.add("eval",	func)
			.add("args", 	args)
			.add("nolock", 	noLock)
			.run["retval"]
	}

	** Executes the given function passing in a database (connection) that has been authenticated 
	** with the given user. Within the function, the authenticated database may be used as often 
	** as you wish.
	** 
	**   data := db.authenticate("ZeroCool", "password") |authDb -> Obj?| {
	** 
	**       return authDb["top-secret"].findAll
	**   }
	** 
	** All Mongo objects ( 'Collection', 'Index', 'User', etc...) created from the authenticated
	** database will inherit the user credentials.
	Obj? authenticate(Str userName, Str password, |Database db->Obj?| func) {
		// run outside the closure so we only ever lease one connection at a time
		return conMgr.leaseConnection |connection->Obj?| {
			connection.authenticate(name, userName, password)
			try {
				cm := ConnectionManagerLocal(connection, conMgr.mongoUri)
				db := Database(cm, name)
				return func.call(db)
			
			} finally {
				// don't interfere with any other exception in progress
				// the connection is closed on Err anyway, so nothing to worry about if this fails.
				connection.logout(name, false)
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
		Collection(this, collectionName) { it.writeConcern = this.writeConcern }
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
		User(conMgr, name, userName) { it.writeConcern = this.writeConcern }
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
	
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		name
	}
}
