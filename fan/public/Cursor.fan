
** Iterates over database query results. See [Collection.find()]`afMongo::Collection.find`.
**
** @see `http://docs.mongodb.org/manual/core/cursors/`
class Cursor {
	private static const Log	_log			:= Utils.getLog(Cursor#)
	private OneShotLock			_querySent	:= OneShotLock("Query has been sent to MongoDB")
	private OneShotLock			_deadCursor	:= OneShotLock("Cursor has been killed")
	private Connection			_connection
	private Namespace			_nsCol
	private Int					_cursorId
	private [Str:Obj?][]?		_results
	private Int 				_resultIndex
	private Int 				_downloaded
	private Int 				_indexLocal

	** Use in 'orderBy' maps to denote sort order.
	static const Int ASC		:= 1
	
	** Use in 'orderBy' maps to denote sort order.
	static const Int DESC		:= -1
	
	** The query as used by this cursor. 
	Str:Obj? query {
		private set
	}
	
	** The number of documents to be returned in each response from the server.
	** 
	** Leave as 'null' to use the default size.
	** 
	** This value can not be changed once a query has been sent to the server.
	Int? batchSize {
		set { _querySent.check; &batchSize = it }
	}
	
	** The maximum number of documents this cursor will read.
	** 
	** Leave as 'null' or set to zero to read all results from the query.
	** 
	** This value can not be changed once a query has been sent to the server.
	Int? limit {
		set { _querySent.check; &limit = it?.max(0) }		
	}
	
	** The number of documents to omit, when returning the result of the query.
	** 
	** Leave as '0' to return all documents.
	** 
	** This value can not be changed once the query has been sent to the server.
	Int skip {
		set { _querySent.check; &skip = it.max(0); &index = it }
	}
	
	** Use to alter / set which fields returned in the Mongo responses.
	** 
	** Set field names with a value of '1' to limit returned fields to just those mentioned. Example:
	** 
	** pre>
	** syntax: fantom
	** cursor.projection = [
	**     "fieldName1" : 1,
	**     "fieldName2" : 1
	** ]
	** <pre
	** 
	** Would limit the returned fields to just '_id', 'fieldName1', & 'fieldName2'.
	** 
	** Leave as 'null' to return all fields. 
	** 
	** See [Projection Operators]`https://docs.mongodb.com/manual/reference/operator/projection/` for other uses.
	** 
	** This value can not be changed once the query has been sent to the server.
	[Str:Obj?]? projection {
		set { _querySent.check; &projection = it }
	}
	
	** Optional flags to set in the query. 
	** 
	** This value can not be changed once the query has been sent to the server.
	OpQueryFlags flags {
		set { _querySent.check; &flags = it }		
	}
	
	** A zero based index into the documents returned by the query.
	** 
	** pre>
	** syntax: fantom
	** cursor.count  // --> 10
	** cursor.skip = 2
	** cursor.index  // -->  2
	** cursor.next
	** cursor.index  // -->  3
	** <pre
	Int index { private set }
	
	** Query modifiers to use. Synonymous to using '_addSpecial()' in the mongo shell.
	** 
	** This value can not be changed once the query has been sent to the server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query-modifier/`
	[Str:Obj?] special {
		get { _querySent.locked ? &special.ro : &special}
		set { _querySent.check  ; &special = it }
	}

	** The name of the index to use for sorting.
	** 
	** This value can not be changed once the query has been sent to the server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/meta/hint/`
	Str? hint {
		get { special["\$hint"] }		
		set { _querySent.check; special["\$hint"] = it }
	}

	** Use to order the query results in ascending or descending order.
	** 
	** If 'orderBy' is a '[Str:Obj?]' map, it should be a document with field names as keys. 
	** Values may either be the standard Mongo '1' and '-1' for ascending / descending or the 
	** strings 'ASC' / 'DESC'.
	** Should 'orderBy' contain more than 1 entry, it must be ordered.
	** 
	** Examples:
	**   syntax: fantom
	** 
	**   cursor.orderBy = ["age": 1]
	**   cursor.orderBy = [:] { ordered = true }.add("name", "asc").add("age", "desc")
	** 
	** This value can not be changed once the query has been sent to the server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/meta/orderby/`
	[Str:Obj?]? orderBy {
		get { _querySent.locked ? (([Str:Obj?]?) special["\$orderby"])?.ro : special["\$orderby"] }
		// convert here with no check, 'cos what is invalid today maybe valid tomorrow. 
		set {
			_querySent.check
			if (it.size > 1 && it.ordered == false)
				throw ArgErr(ErrMsgs.cursor_mapNotOrdered(it))
			special["\$orderby"] = Utils.convertAscDesc(it) 
		}
	}
	
	internal new make(Connection connection, Namespace namespace, Str:Obj? query) {
		this._connection = connection		
		this._nsCol		= namespace
		this.query		= query
		this.flags		= OpQueryFlags.none
		this.special	= _cmd
	}

	** Used from Collection.aggregate()
	internal new makeFromId(Connection connection, Namespace namespace, Str:Obj? query, Int cursorId, [Str:Obj?][] results) {
		this._connection = connection		
		this._nsCol		= namespace
		this.query		= query
		this.flags		= OpQueryFlags.none
		this.special	= _cmd
		
		this._querySent.lock
		this._cursorId	= cursorId
		this._results	= results
	}
	
	** Returns the next document from the query.
	** Use with 'hasNext()' to iterate over the results:
	** 
	** pre>
	** syntax: fantom
	** 
	** while (cursor.hasNext) {
	**     doc := cursor.next
	**     ...
	** }
	** <pre
	** 
	** If 'checked' is 'true' and there are no more results to return then an 'MongoCursorErr' is 
	** thrown, else 'null' is returned.
	[Str:Obj?]? next(Bool checked := true) {
		// leave it to 'getSome' and 'getMore' to do the dead cursor check
		if (_results == null)
			getSome
		else if (_resultIndex >= _results.size) {
			if (_deadCursor.locked)
				return null ?: (checked ? throw MongoCursorErr(ErrMsgs.cursor_noMoreData) : null)
			_getMore(false)
		}
		if (_resultIndex >= _results.size)
			return null ?: (checked ? throw MongoCursorErr(ErrMsgs.cursor_noMoreData) : null)
		result := _results[_resultIndex++]
		index++
		_indexLocal++
		return result
	}

	** Are more documents to be returned?
	** Use with 'next()' to iterate over the results:
	** 
	** pre>
	** syntax: fantom
	** 
	** while (cursor.hasNext) {
	**     doc := cursor.next
	**     ...
	** }
	** <pre
	Bool hasNext() {
		_indexLocal < _maxDownload
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
		// if nothing has been returned, ask for some data
		if (!_querySent.locked) {
			flags = OpQueryFlags.exhaust + flags
			batchSize = null
			getSome
		}

		// if all the results have been read, then just return a (subset) of results
		if (_querySent.locked && !isAlive)
			return (_resultIndex == 0) ? _results : _results[_resultIndex..-1]

		// we're in the middle of iterating, so...
		// cull any document already seen
		_results = _results[_resultIndex..-1]
		
		// read the rest
		_getMore(true)
		return _results
	}
	
	** Returns the maximum number of documents this query can return. 
	** 'count' is constant for any given query and *is* affected by 'skip' or 'limit'. 
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/count/`
	once Int count() {
		_runCmd(_cmd
			.add("count", _nsCol.collectionName)
			.add("query", query)
			.add("hint", orderBy ?: hint)
			.add("limit", limit ?: 0)
			.add("skip", skip)
		)["n"]->toInt
	}

	** Returns 'true' if the cursor is alive on the server.
	** 
	** Note this returns 'false' if a query has not yet been sent to the server.
	Bool isAlive() {
		_querySent.locked && _cursorId != 0
	}
	
	// ---- Helper Methods ------------------------------------------------------------------------

	internal Void kill() {
		if (isAlive) {
			Operation(_connection).killCursors([_cursorId])
			_cursorId = 0
			_deadCursor.lock
		}
	}

	internal Void getSome() {
		// drain isn't passed in, 'cos it may be user set
		drain := &flags.containsAll(OpQueryFlags.exhaust)

		_deadCursor.check
		
		// we want to limit the no. of returned results to the smallest, non-null value
		qlimit := [&batchSize, &limit].sort.find { it != null } ?: 0
		
		// if we're bringing down the entire limit, negate so the server doesn't keep the cursor 
		// open and exhaust *everything*!
		if (qlimit == &limit)
			qlimit = -&limit
		
		reply := Operation(_connection).query(_nsCol.qname, _compileQuery, qlimit, &skip, &projection, &flags)
		_querySent.lock
		_gotSome(reply, false)
		
		// if an 'exhaust' query then gulp down all the server replies
		if (drain) {
			while (isAlive) {
				reply = Operation(_connection).readReply(null)
				_gotSome(reply, true)
			}
		}
	}

	private Void _getMore(Bool drain) {
		_deadCursor.check

		// make sure the cursor doesn't ever bring down more than 'limit'
		qlimit := _getMoreQlimit
		reply := Operation(_connection).getMore(_nsCol.qname, qlimit, _cursorId)
		_gotSome(reply, false)
		
		if (drain) {
			while (isAlive && (_downloaded < _maxDownload)) {
				reply = Operation(_connection).getMore(_nsCol.qname, _getMoreQlimit, _cursorId)
				_gotSome(reply, true)
			}
			// and we're spent!
			kill
		}
	}

	private Void _gotSome(OpReplyResponse reply, Bool append) {
		_cursorId 	= reply.cursorId
		_downloaded	+= reply.documents.size

		if (append) {
			_results.addAll(reply.documents)
		} else {
			_resultIndex	= 0			
			_results 	= reply.documents
			if (index != (reply.cursorPos + skip))
				_log.warn(LogMsgs.cursor_indexOutOfSync(index, reply.cursorPos + skip))
		}

		if (!isAlive)
			_deadCursor.lock
	}
	
	private Int _getMoreQlimit() {
		qlimit := batchSize ?: 0
		if (limit != null) {
			qlimit = limit - _downloaded
		}
		return qlimit
	}
	
	private Int _maxDownload() {
		limit ?: count
	}
	
	private [Str:Obj?] _compileQuery() {
		special.isEmpty ? query : _cmd.add("\$query", query).addAll(special)
	}
	
	private Str:Obj? _cmd() {
		Str:Obj?[:] { ordered = true }
	}	
	
	private Str:Obj? _runCmd(Str:Obj? cmd) {
		Operation(_connection).runCommand("${_nsCol.databaseName}.\$cmd", cmd)
	}

	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		_nsCol.qname
	}
}
