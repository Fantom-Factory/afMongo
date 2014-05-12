
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
	new make(ConnectionManager connectionManager, Str name) {
		this.conMgr = connectionManager
		this.name = Namespace.validateDatabaseName(name)
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
		userNs := Namespace(name, "system.users")
		c:= Collection(conMgr, userNs).findAll	//(["ns":namespace.qname]).map { it["name"] }.sort
		Env.cur.err.printLine(c)
		return [,]
	}
	
	** Returns a 'User' with the given name.
	** 
	** Note this just instantiates the Fantom object, it does not create anything in the database. 
	User user(Str userName) {
		User(conMgr, name, userName)
	}
	
	** Drops ALL users from this database. *Be careful!*
	**
	** @see `http://docs.mongodb.org/manual/reference/command/dropAllUsersFromDatabase/`
	This dropAllUsers() {
		c:=cmd.add("dropAllUsersFromDatabase", 1).run
		Env.cur.err.printLine(c)
		return this
	}
	
	// ---- Database ------------------------------------------------------------------------------

	// FIXME: create!
	
	** Drops the database. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropDatabase/`
	This drop() {
		c:=cmd.add("dropDatabase", 1).run
		Env.cur.err.printLine(c)
		return this
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, Namespace(name, "wotever"), action)
	}
}
