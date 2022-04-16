
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
	
	** A configurable timeout.
	Duration?			maxTime
	
	private [Str:Obj?][]?	_batch
	private Int 			_batchIndex
	private Int 			_totalIndex

	** Creates a new Mongo Cursor with a first batch
	new make(MongoConnMgr connMgr, Str dbName, Str colName, Int cursorId, [Str:Obj?][]? firstBatch := null) {
		this.connMgr	= connMgr
		this.dbName		= dbName
		this.colName	= colName
		this.cursorId	= cursorId
		this._batch		= firstBatch
	}

	
	
	** Iterates over all *remaining* and unread documents.
	** 
	** The cursor is automatically killed.
	** 
	** pre>
	** syntax: fantom
	** 
	** cursor.each |Str:Obj? doc, Int index| {
	**     ...
	** }
	** <pre
	Void each(|Str:Obj? doc, Int index| c) {
		throw Err()
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
		
		if (_batchIndex >= _batch.size) {
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
	
	Void kill() {
		if (isAlive == false)
			return
		
		throw Err()
	}
	
	** Return all *remaining* and unread documents as a List.
	** 
	** pre>
	** syntax: fantom
	** 
	** cursor.count  // --> 10
	** cursor.skip = 2
	** cursor.next
	** cursor.next
	** list := cursor.toList
	** list.size    // -->  6
	** <pre
	[Str:Obj?][] toList() {
		
		// FIXME
		throw UnsupportedErr()
	}
	
	** Returns 'true' if the cursor is alive on the server.
	Bool isAlive() {
		cursorId != 0
	}

//	internal Void kill() {
//		if (isAlive) {
//			Operation(_connection).killCursors([_cursorId])
//			_cursorId = 0
//			_deadCursor.lock
//		}
//	}
}
