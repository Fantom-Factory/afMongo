
** Represents a MongoDB database.
** 
** http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-driver-requirements/
** http://docs.mongodb.org/meta-driver/latest/legacy/feature-checklist-for-mongodb-drivers/
** 
** http://docs.mongodb.org/manual/reference/command/
** http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
const class Database {
	const Str name

	internal const ConnectionManager conMgr
	  
	new make(ConnectionManager connectionManager, Str name) {
		this.conMgr = connectionManager
		this.name = Namespace.validateDatabaseName(name)
	}

	Collection collection(Str collectionName) {
		Collection(this, collectionName)
	}

	@Operator
	Collection get(Str collectionName) {
		collection(collectionName)
	}
	
	Str[] collectionNames() {
		// if it wasn't for F4, I could have this all on one line!
		docs  := collection("system.namespaces").findAll
		names := (Str[]) docs.map |ns->Str| { ns["name"] }
		return names.exclude { !it.startsWith(name) || it.contains("\$") || it.contains(".system.") }.map { it[(name.size+1)..-1] }.sort
	}
	
	// FIXME: create!
	
	** Drops the database. *Be careful!*
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropDatabase/`
	This drop() {
		runCmd(["dropDatabase":1])
		return this
	}
	
	// ---- Private Methods ----
	
	private Str:Obj? runCmd(Str:Obj? cmd) {
		conMgr.leaseConnection |con->Obj?| {
			Operation(con).runCommand("${name}.\$cmd", cmd)
		}
	}

//	private Str:Obj? runAdminCmd(Str:Obj? cmd) {
//		Operation(conMgr.getConnection).runCommand("admin.\$cmd", cmd)
//	}
}
