
** Mongo Cursors iterate over Collection data.
** 
** @See
**  - `https://www.mongodb.com/docs/manual/reference/command/getMore/`
** 	- `https://www.mongodb.com/docs/manual/reference/command/killCursors/`
**  - `https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst`
class MongoCur {
	
	** The connection manager that Mongo connections are leased from.
	const MongoConnMgr	connMgr
	
	** The database this cursors iterates in.
	const Str			dbName
	
	** The collection this cursors iterates over.
	const Str			colName
	
	** The cursor ID.
	Int					cursorId {
		private set
	}
	
	** A configurable batch size.
	Int?				batchSize {
		set {
			if (it == 0) it = null
			if (it != null && it < 0) it = it.abs
			&batchSize = it
		}
	}
	
	** A configurable timeout for Mongo server operations.
	Duration?			maxTime {
		set {
			if (it == 0ms) it = null
			if (it != null && it < 0ms) it = it.abs
			&maxTime = it
		}
	}
	
	private [Str:Obj?][]?	_batch
	private Int 			_batchIndex
	private Int 			_totalIndex

	** Creates a new Mongo Cursor with a first batch
	new make(MongoConnMgr connMgr, Str dbName, Str colName, Int cursorId, [Str:Obj?][]? firstBatch := null) {
		this.connMgr		= connMgr
		this.dbName			= dbName
		this.colName		= colName
		this.cursorId		= cursorId
		this._batch			= firstBatch
		this._totalIndex	= -1
	}

	** Iterates over all *remaining* and unread documents.
	** 
	** This cursor is guaranteed to be killed.
	** 
	** pre>
	** syntax: fantom
	** 
	** cursor.each |Str:Obj? doc, Int index| {
	**     ...
	** }
	** <pre
	Void each(|Str:Obj? doc, Int index| fn) {
		if (isAlive == false)
			return

		doc  := null as Str:Obj?
		try while ((doc = next) != null) {
			fn(doc, _totalIndex)
		}
		finally kill
	}	
	
	** Returns the next document from the cursor, or 'null'.
	** 
	** The cursor must be manually killed.
	** pre>
	** syntax: fantom
	** 
	** doc := null as Str:Obj?
	** while ((doc = cursor.next) != null) {
	**     ...
	** }
	** cursor.kill()
	** <pre
	[Str:Obj?]? next(Bool checked := true) {
		if (isAlive == false)
			return null
		
		if (_isExhausted) {
			cur := MongoCmd(connMgr, dbName, "getMore", cursorId)
				.add("collection",	colName)
				.add("batchSize",	batchSize)
				.add("maxTimeMS",	maxTime?.toMillis)
				.run["cursor"] as Str:Obj?
			
			this.cursorId	= cur["id"]
			this._batch		= cur["nextBatch"]
			this._batchIndex= 0
		}
		
		_totalIndex++
		return _batch[_batchIndex++]
	}
	
	** Kills this cursor.
	** 
	** No more documents will be returned from 'next()', 'each()', or 'toList()'.
	Void kill() {
		if (isAlive == false)
			return
		
		res := MongoCmd(connMgr, dbName, "killCursors", colName)
			.add("cursors",		[cursorId])
			.run
		
		// there is no coming back from a Kill Cmd!
		this.cursorId		= 0
		this._batchIndex	= _batch.size
		
		// double check and log a warning if something seems afoot
		killed := res["cursorsKilled"] as Int[]
		if (killed.contains(cursorId) == false)
			connMgr.log.warn("Cursor (${cursorId}) not killed. Mongo says:\n" + BsonPrinter().print(res))
	}
	
	** Return all *remaining* and unread documents as a List.
	** 
	** Returns an empty list should the cursor be killed.
	[Str:Obj?][] toList() {
		if (isAlive == false)
			return Str:Obj?[]#.emptyList
		
		docs := Str:Obj?[][,]
		docs.capacity = _batch.size
		doc  := null as Str:Obj?
		while ((doc = next) != null) {
			docs.add(doc)
		}

		return docs
	}
	
	** Returns 'true' if the cursor is alive on the server.
	Bool isAlive() {
		isDead := _isExhausted && cursorId == 0
		return isDead == false
	}
	
	private Bool _isExhausted() {
		_batchIndex >= _batch.size
	}

	@NoDoc
	override Str toStr() { cursorId.toStr }
}
