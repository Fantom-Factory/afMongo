
internal class MongoCmd {
	private const ConnectionManager conMgr
	private const Namespace			namespace

	private [Str:Obj?] 	cmd	:= Str:Obj?[:] { ordered = true }
	private Bool 		checkForWriteErrs
	private Str? 		when
	private Str? 		what
	
	new make(ConnectionManager conMgr, Namespace namespace, Str? action) {
		this.conMgr				= conMgr
		this.namespace 			= namespace
		this.checkForWriteErrs	= action != null
		
		switch (action) {
			case null:
				null?.toStr
			case "cmd":
				this.when	= "commanding"
				this.what	= "happened"
			case "insert":
				this.when	= "when inserting into"
				this.what	= "was inserted"
			case "update":
				this.when	= "when updating"
				this.what	= "was updated"
			case "delete":
				this.when	= "when deleting from"
				this.what	= "was deleted"
			default:
				throw ArgErr("Unknown action: $action")
		}
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
		// when checking for write errs, we'll also check for okay
		errIfNotOkay := !checkForWriteErrs
		
		doc := (Str:Obj?) conMgr.leaseConnection |con->Str:Obj?| {
			Operation(con).runCommand("${namespace.databaseName}.\$cmd", cmd, errIfNotOkay)
		}

		if (errIfNotOkay) return doc

		// check for write errs before we throw a generic 'Not Ok' err
		errs := [Str:Obj?][,]
		if (doc.containsKey("writeErrors"))
			errs.addAll((Obj?[]) doc["writeErrors"])
		if (doc.containsKey("writeConcernError"))
			errs.add((Str:Obj?) doc["writeConcernError"])
		if (!errs.isEmpty)
			throw MongoCmdErr(MongoErrMsgs.cmd_writeErrs(when, namespace.qname, errs), errs)
		
		if (doc["ok"] != 1f && doc["ok"] != 1) {
			// attempt to work out the cmd, usually the first key in the given doc
			cname := cmd.keys.first
			throw MongoCmdErr(MongoErrMsgs.operation_cmdFailed(cname, doc["errmsg"] ?: doc), [doc])
		}

		// After some deliberation I decided not to check these results. 
		// For updates at least, it means each doc would need a 'dirty' flag
		// - which is always tricky to implement.
		
//		// it's handy that null != 0, means we don't blow up if 'n' doesn't exist!
//		if (doc["n"]?->toInt == 0)
//			throw MongoErr(ErrMsgs.cmd_nothingHappened(what, doc))
//		if (doc["nModified"]?->toInt == 0)
//			throw MongoErr(ErrMsgs.cmd_nothingHappened(what, doc))

		return doc
	}
}