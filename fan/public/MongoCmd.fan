
class MongoCmd {
	private const ConnectionManager conMgr
	private const Str				dbName

	private [Str:Obj?] 	cmd	:= Str:Obj?[:] { ordered = true }
	
	internal new make(ConnectionManager conMgr, Str dbName) {
		this.conMgr		= conMgr
		this.dbName 	= dbName
	}

	** If 'val' is null, it is not added. Handy for chaining 'add()' methods.
	This add(Str key, Obj? val) {
		if (val != null)
			cmd[key] = val
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
		return cmd.containsKey(key)
	}	
	
	Str:Obj? query() { cmd }
	
	Str:Obj? run() {
		doc := (Str:Obj?) conMgr.leaseConnection |con->Str:Obj?| {
			Operation(con).runCommand(dbName, cmd)
		}


		return doc
	}
}