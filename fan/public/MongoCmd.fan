
** The principle class that communicates with MongoDB.
class MongoCmd {
	const MongoConnMgr	connMgr
	const Str			dbName
	const Str			cmdName
	const Obj?			cmdVal
	private MongoSess?	session

	** The backing cmd document.
	Str:Obj? cmd {
		private set
	} 
	
	** Creates a new 'MongoCmd'.
	new make(MongoConnMgr connMgr, Str dbName, Str cmdName, Obj? cmdVal := 1, Obj? session := null) {
		this.connMgr	= connMgr
		this.dbName 	= dbName
		this.cmdName	= cmdName
		this.cmdVal		= cmdVal
		this.session	= session
		this.cmd		= Str:Obj?[:] { ordered = true } 
		this.add(cmdName, cmdVal)
	}

	** Adds the given 'val' - but only if it does not aleady exist in the cmd.
	** 
	** If 'val' is null, it is not added.
	** 
	** Use to chain 'add()' methods.
	This add(Str key, Obj? val) {
		if (val != null && cmd.containsKey(key) == false)
			cmd.add(key, val)
		return this
	}	

	** Adds all the given vals - but only if they don't already exist in the cmd.
	** 
	** If 'all' is null, it is not added.
	** 
	** Use to chain 'add()' methods.
	This addAll([Str:Obj?]? all) {
		if (all != null)
			all.each |v, k| { this.add(k, v) }
		return this
	}	
	
	** Like 'with()', but 'fn' may be 'null'.
	This withFn(|MongoCmd|? fn) {
		fn?.call(this)
		return this
	}

	** Returns 'true' if this cmd contains the given key.
	Bool containsKey(Str key) {
		cmd.containsKey(key)
	}	

	Str:Obj? extract(Str[] keys) {
		map := Str:Obj?[:]
		map.ordered = true
		for (i := 0; i < keys.size; ++i) {
			key := keys[i]
			val := cmd.remove(key)
			if (val != null)
				map[key] = val			
		}
		return map
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
	
	@NoDoc
	override Obj? trap(Str name, Obj?[]? args := null) {
		if (args == null || args.isEmpty)
			return get(name)
		if (args.size == 1)
			return add(name, args.first)
		throw UnsupportedErr("MongoCmd->${name}(${args})")
	}

	** Executes this cmd on the MongoDB server, and returns the response as a BSON document.
	Str:Obj? run(Bool checked := true) {
		doc := (Str:Obj?) connMgr.leaseConn |con->Str:Obj?| {
			con.setSession(session)
			return MongoOp(con, cmd).runCommand(dbName, checked)
		}
		return doc
	}
	
	** Executes this cmd on the MongoDB server, and preemptively interprets the response as a cursor.
	MongoCur cursor() {
		connMgr.leaseConn |con->MongoCur| {
			doc		:= MongoOp(con, cmd).runCommand(dbName)
			cur		:= doc["cursor"] as Str:Obj?
			curId	:= cur["id"]
			sess	:= curId == 0 ? null : con.detachSession
			return MongoCur(connMgr, dbName, cmdVal.toStr, curId, cur["firstBatch"], sess)
		}
	}
}
