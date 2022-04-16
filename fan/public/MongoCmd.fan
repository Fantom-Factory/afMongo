
class MongoCmd {
	const MongoConnMgr	connMgr
	const Str			dbName
	const Str			cmdName
	const Obj?			cmdVal

	Str:Obj? cmd {
		private set
	} 
	
	new make(MongoConnMgr connMgr, Str dbName, Str cmdName, Obj? cmdVal := 1) {
		this.connMgr	= connMgr
		this.dbName 	= dbName
		this.cmdName	= cmdName
		this.cmdVal		= cmdVal
		this.cmd		= Str:Obj?[:] { ordered = true } 
		this.add(cmdName, cmdVal)
	}

	** If 'val' is null, it is not added. Handy for chaining 'add()' methods.
	This add(Str key, Obj? val) {
		if (val != null)
			cmd.add(key, val)
		return this
	}	

	** If 'all' is null, it is not added. Handy for chaining 'add()' methods.
	This addAll([Str:Obj?]? all) {
		if (all != null)
			cmd.addAll(all)
		return this
	}	

	@Operator
	Obj? get(Str key) {
		cmd[key]
	}
	
	@Operator
	This set(Str key, Obj? val) {
		cmd[key] = val
		return this
	}	

	Bool containsKey(Str key) {
		cmd.containsKey(key)
	}	
	
	Str:Obj? run(Bool checked := true) {
		doc := (Str:Obj?) connMgr.leaseConn |con->Str:Obj?| {
			MongoOp(con).runCommand(dbName, cmd, checked)
		}
		return doc
	}
	
	MongoCur cursor() {
		cur := run()["cursor"] as Str:Obj?
		return MongoCur(connMgr, dbName, cmdVal.toStr, cur["id"], cur["firstBatch"])
	}
}