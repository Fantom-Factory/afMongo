
** Represents a MongoDB database.
** 
** http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-driver-requirements/
** http://docs.mongodb.org/meta-driver/latest/legacy/feature-checklist-for-mongodb-drivers/
** 
** http://docs.mongodb.org/manual/reference/command/
** http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
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
