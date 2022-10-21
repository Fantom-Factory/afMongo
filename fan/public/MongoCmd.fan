
** The principle class that communicates with MongoDB.
class MongoCmd {
	private MongoSess?	session

	** The connection manager that Mongo connections are leased from.
	const MongoConnMgr	connMgr

	** The name of the database.
	const Str			dbName
	
	** The name of this cmd.
	const Str			cmdName
	
	** The value of this cmd.
	const Obj?			cmdVal

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
		this.session	= session	// cursors and txns need to specify a session
		this.cmd		= Str:Obj?[:]
		this.cmd.ordered= true
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
	@NoDoc
	This withFn(|MongoCmd|? fn) {
		fn?.call(this)
		return this
	}

	** Returns 'true' if this cmd contains the given key.
	Bool containsKey(Str key) {
		cmd.containsKey(key)
	}

	** Extracts the given keys into a Map.
	@NoDoc
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
	
	** Returns a value from the cmd.
	@Operator
	Obj? get(Str key) {
		cmd[key]
	}
	
	** Sets a value in the cmd.
	@Operator
	This set(Str key, Obj? val) {
		cmd[key] = val
		return this
	}	
	
	** Trap operator for 'get()' and 'add()'.
	override Obj? trap(Str name, Obj?[]? args := null) {
		if (args == null || args.isEmpty)
			return get(name)
		if (args.size == 1)
			return add(name, args.first)
		throw UnsupportedErr("MongoCmd->${name}(${args})")
	}

	** Executes this cmd on the MongoDB server, and returns the response as a BSON document.
	Str:Obj? run(Bool checked := true) {
		doc := (Str:Obj?) connMgr.leaseConn |conn->Str:Obj?| {
			if (this.session != null)
				conn._setSession(session)
			try return MongoOp(connMgr, conn, cmd).runCommand(dbName, checked)
			finally	// don't let detatched sessions get checked back in!
				if (this.session != null || this.session?.isDetached == true)
					conn._setSession(null)
		}
		return doc
	}
	
	** Executes this cmd on the MongoDB server, and preemptively interprets the response as a cursor.
	MongoCur cursor() {
		connMgr.leaseConn |conn->MongoCur| {
			doc		:= MongoOp(connMgr, conn, cmd).runCommand(dbName)
			cur		:= doc["cursor"] as Str:Obj?
			curId	:= cur["id"]
			sess	:= curId == 0 ? null : conn._detachSession
			cursor	:= MongoCur(connMgr, dbName, cmdVal.toStr, curId, cur["firstBatch"], sess)
			// these values need to be set per request
			if (cmd["batchSize"] != null)
				cursor.batchSize = cmd["batchSize"] as Int
			if (cmd["maxTimeMS"] != null)
				cursor.maxTime = (cmd["maxTimeMS"] as Int)?.mult(1ms.ticks)?.toDuration
			return	cursor
		}
	}
}
