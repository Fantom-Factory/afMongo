
const class Indexes {

	private const Namespace			colNs
	private const Namespace			idxNs
	private const ConnectionManager conMgr
	
	internal new make(ConnectionManager conMgr, Namespace namespace) {
		this.conMgr	= conMgr
		this.colNs	= namespace
		this.idxNs	= Namespace(colNs.databaseName, "system.indexes")
	}
	
	** Returns a list of index names.
	Str[] names() {
		Collection(conMgr, idxNs.qname).findAll(["ns":colNs.qname]).map { it["name"] }.sort
	}

	** Returns info on the named index.
	** 
	** @see `http://docs.mongodb.org/manual/reference/method/db.collection.getIndexes/#db.collection.getIndexes`
	Str:Obj? info(Str indexName) {
		Collection(conMgr, idxNs.qname).findOne(["ns":colNs.qname, "name":indexName])
	}
	
	** Returns 'true' if the named index exists.
	Bool exists(Str indexName) {
		Collection(conMgr, idxNs.qname).findOne(["ns":colNs.qname, "name":indexName], false) != null		
	}
	
	** Drops a named index.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	Void drop(Str indexName) {
		runCmd(cmd.add("dropIndexes", colNs.collectionName).add("index", indexName))
	}

	** Drops ALL indexes on the collection.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/dropIndexes/`
	Void dropAll() {
		runCmd(cmd.add("dropIndexes", colNs.collectionName).add("index", "*"))
	}
	
	** Creates an index.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/createIndexes/`
	Void create(Str name, Str:Obj? key, Str:Obj options) {
		// there's no createIndexMulti 'cos I figure no novice will need to create multiple indexes at once!
		runCmd(cmd
			.add("createIndexes", colNs.collectionName)
			.add("indexes", 	[cmd
				.add("key",		key)
				.add("name",	name)
				.addAll(options)
			])
		)
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Str:Obj? cmd() {
		Str:Obj?[:] { ordered = true }
	}	
	
	private Str:Obj? runCmd(Str:Obj? cmd) {
		conMgr.leaseConnection |con->Obj?| {
			Operation(con).runCommand("${colNs.databaseName}.\$cmd", cmd)
		}
	}

	private Str:Obj? runAdminCmd(Str:Obj? cmd) {
		conMgr.leaseConnection |con->Obj?| {
			Operation(con).runCommand("admin.\$cmd", cmd)
		}
	}
}
