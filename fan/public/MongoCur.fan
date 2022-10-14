using afBson::BsonIO

** Mongo Cursors iterate over Collection data.
** 
** @See
**  - `https://www.mongodb.com/docs/manual/reference/command/getMore/`
** 	- `https://www.mongodb.com/docs/manual/reference/command/killCursors/`
**  - `https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst`
class MongoCur {
	
	** The underlying connection manager.
	const MongoConnMgr	connMgr
	
	** The database this cursor iterates in.
	const Str			dbName
	
	** The collection this cursor iterates over.
	const Str			colName
	
	** The cursor ID.
	Int					cursorId {
		private set
	}
	
	** A configurable batch size.
	** 
	** 'null' indicates a Mongo default of '101'.
	Int?				batchSize {
		set {
			if (it == 0) it = null
			if (it != null && it < 0) it = it.abs
			&batchSize = it
		}
	}
	
	** A configurable timeout for Mongo server operations.
	** 
	** 'null' indicates a Mongo default of forever.
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
	private MongoSess?		_session

	** Creates a new Mongo Cursor with a first batch
	new make(MongoConnMgr connMgr, Str dbName, Str colName, Int cursorId, [Str:Obj?][]? firstBatch := null, Obj? session := null) {
		this.connMgr		= connMgr
		this.dbName			= dbName
		this.colName		= colName
		this.cursorId		= cursorId
		this._batch			= firstBatch
		this._session		= session
		this._totalIndex	= -1
	}
	
	** Returns the next document from the cursor, or 'null'.
	** 
	** The cursor must be manually killed.
	** pre>
	** syntax: fantom
	** 
	** while (cursor.isAlive) {
	**     doc := cursor.next
	**     ...
	** }
	** cursor.kill
	** <pre
	[Str:Obj?]? next() {
		if (isAlive == false)
			return null
		
		if (_isExhausted) {
			cur := MongoCmd(connMgr, dbName, "getMore", cursorId, _session)
				.add("collection",	colName)
				.add("batchSize",	batchSize)
				.add("maxTimeMS",	maxTime?.toMillis)
				.run["cursor"] as Str:Obj?
			
			this.cursorId	= cur["id"]
			this._batch		= cur["nextBatch"]
			this._batchIndex= 0
			
			if (this.cursorId == 0) {
				this._session?.endSession
				this._session = null
			}

			// very occasionally, we get an "IndexErr: 0" when returning _batch data,
			// presumably after getMore() returns 0 documents
			if (isAlive == false)
				return null
		}
		
		_totalIndex++
		return _batch[_batchIndex++]
	}
	
	** Returns the current index of the last document.
	Int index() {
		_totalIndex
	}
	
	** Kills this cursor.
	** 
	** No more documents will be returned from 'next()', 'each()', or 'toList()'.
	Void kill() {
		if (cursorId == 0)
			return
		
		res := MongoCmd(connMgr, dbName, "killCursors", colName, _session)
			.add("cursors",	[cursorId])
			.run
		
		// there is no coming back from a Kill Cmd!
		this.cursorId		= 0
		this._batchIndex	= _batch.size
		
		// double check and log a warning if something seems afoot
		killed := res["cursorsKilled"] as Int[]
		if (killed.contains(cursorId) == false)
			connMgr.log.warn("Cursor (${cursorId}) not killed. Mongo says:\n" + BsonIO().print(res))
		
		this._session?.endSession
		this._session = null
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
		try while (isAlive) {
			doc := next
			if (doc != null)
				fn(doc, _totalIndex)
		}
		finally kill
	}

	** Converts all *remaining* and unread documents to a List.
	** The new list is typed based on the return type of the function.
	** 
	** This cursor is guaranteed to be killed.
	** 
	** pre>
	** syntax: fantom
	** 
	** list := cursor.map |Str:Obj? doc, Int index -> Obj?| {
	**     ...
	** }
	** <pre
	Obj?[] map(|Str:Obj? doc, Int index->Obj?| fn) {
		type := fn.returns == Void# ? Obj?# : fn.returns
		list := List.make(type, 16)
		try while (isAlive) {
			doc := next
			if (doc != null) {
				obj := fn(doc, _totalIndex)
				list.add(obj)
			}
		}
		finally kill
		return list
	}

	** Return all *remaining* and unread documents as a List.
	** 
	** This cursor is guaranteed to be killed.
	** 
	** Returns an empty list should the cursor be killed.
	[Str:Obj?][] toList() {
		docs := Str:Obj?[][,]
		docs.capacity = _batch.size	// it's a good starting point!
		try while (isAlive) {
			doc := next
			if (doc != null)
				docs.add(doc)
		}
		finally kill
		return  docs
	}
	
	** Returns 'true' if the cursor is alive on the server or more documents may be read.
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
