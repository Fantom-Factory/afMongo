
** Iterates over database query results.
**
** @see `http://docs.mongodb.org/manual/core/cursors/`
class Cursor {
	private static const Log	log			:= Utils.getLog(Cursor#)
	private OneShotLock			querySent	:= OneShotLock("Query has been sent to MongoDB")
	private OneShotLock			deadCursor	:= OneShotLock("Cursor has been killed")
	private Connection			connection
	private Namespace			nsCol
	private Int					cursorId
	private [Str:Obj?][]?		results
	private Int 				resultIndex
	private Int 				downloaded
	private Int 				indexLocal

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
		set { querySent.check; &batchSize = it }
	}
	
	** The maximum number of documents this cursor will read.
	** 
	** Leave as 'null' to read all results from the query.
	** 
	** This value can not be changed once a query has been sent to the server.
	Int? limit {
		set { querySent.check; &limit = it }		
	}
	
	** The number of documents to omit, when returning the result of the query.
	** 
	** Leave as '0' to return all documents.
	** 
	** This value can not be changed once the query has been sent to the server.
	Int skip {
		set { querySent.check; &skip = it; &index = it }
	}
	
	** The names of the fields to be returned in the query results.
	**  
	** Leave as 'null' to return all fields. 
	** 
	** This value can not be changed once the query has been sent to the server.
	Str[]? fieldNames {
		set { querySent.check; &fieldNames = it }		
	}
	
	** Optional flags to set in the query. 
	** 
	** This value can not be changed once the query has been sent to the server.
	** 
	** @see `OpQueryFlags`
	Flag flags {
		set { querySent.check; &flags = it }		
	}
	
	** A zero based index into the documents returned by the query.
	** 
	** pre>
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
		get { querySent.locked ? &special.ro : &special}
		set { querySent.check  ; &special = it }
	}

	** The name of the index to use for sorting.
	** 
	** This value can not be changed once the query has been sent to the server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/meta/hint/`
	Str? hint {
		get { special["\$hint"] }		
		set { querySent.check; special["\$hint"] = it }
	}

	** Use to sort the query results in ascending or descending order.
	** 
	** If 'sort' is a '[Str:Obj?]' map, it should be a sort document with field names as keys. 
	** Values may either be the standard Mongo '1' and '-1' for ascending / descending or the 
	** strings 'ASC' / 'DESC'.
	** Should 'orderBy' contain more than 1 entry, it must be ordered.
	** 
	** Examples:
	**   cursor.orderBy = ["age": 1]
	**   cursor.orderBy = [:] { ordered = true }.add("name", "asc").add("age", "desc")
	** 
	** This value can not be changed once the query has been sent to the server.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/meta/orderby/`
	[Str:Obj?]? orderBy {
		get { querySent.locked ? (([Str:Obj?]?) special["\$orderby"])?.ro : special["\$orderby"] }
		// convert here with no check, 'cos what is invalid today maybe valid tomorrow. 
		set {
			querySent.check
			if (it.size > 1 && it.ordered == false)
				throw ArgErr(ErrMsgs.cursor_mapNotOrdered(it))
			special["\$orderby"] = Utils.convertAscDesc(it) 
		}
	}
	
	internal new make(Connection connection, Namespace namespace, Str:Obj? query) {
		this.connection = connection		
		this.nsCol		= namespace
		this.query		= query
		this.flags		= OpQueryFlags.none
		this.special	= cmd
	}

	** Used from Collection.aggregate()
	internal new makeFromId(Connection connection, Namespace namespace, Str:Obj? query, Int cursorId, [Str:Obj?][] results) {
		this.connection = connection		
		this.nsCol		= namespace
		this.query		= query
		this.flags		= OpQueryFlags.none
		this.special	= cmd
		
		this.querySent.lock
		this.cursorId	= cursorId
		this.results	= results
	}
	
	** Returns the next document from the query.
	** Use with 'hasNext()' to iterate over the results:
	** 
	** pre>
	** while (cursor.hasNext) {
	**   doc := cursor.next
	**   ...
	** }
	** <pre
	** 
	** If 'checked' is 'true' and there are no more results to return then an 'MongoCursorErr' is 
	** thrown, else 'null' is returned.
	[Str:Obj?]? next(Bool checked := true) {
		// leave it to 'getSome' and 'getMore' to do the dead cursor check
		if (results == null)
			getSome
		else if (resultIndex >= results.size) {
			if (deadCursor.locked)
				return null ?: (checked ? throw MongoCursorErr(ErrMsgs.cursor_noMoreData) : null)
			getMore(false)
		}
		if (resultIndex >= results.size)
			return null ?: (checked ? throw MongoCursorErr(ErrMsgs.cursor_noMoreData) : null)
		result := results[resultIndex++]
		index++
		indexLocal++
		return result
	}

	** Are more documents to be returned?
	** Use with 'next()' to iterate over the results:
	** 
	** pre>
	** while (cursor.hasNext) {
	**   doc := cursor.next
	**   ...
	** }
	** <pre
	Bool hasNext() {
		indexLocal < maxDownload
	}
	
	** Return all *remaining* and unread documents as a List.
	** 
	** pre>
	** cursor.count  // --> 10
	** cursor.skip = 2
	** cursor.next
	** cursor.next
	** list := cursor.toList
	** list.size    // -->  6
	** <pre
	[Str:Obj?][] toList() {
		// if nothing has been returned, ask for some data
		if (!querySent.locked) {
			flags += OpQueryFlags.exhaust
			batchSize = null
			getSome
		}

		// if all the results have been read, then just return a (subset) of results
		if (querySent.locked && !isAlive)
			return (resultIndex == 0) ? results : results[resultIndex..-1]

		// we're in the middle of iterating, so...
		// cull any document already seen
		results = results[resultIndex..-1]
		
		// read the rest
		getMore(true)
		return results
	}
	
	** Returns the maximum number of documents this query can return. 
	** 'count' is constant for any given query and is not affected by 'skip' or 'limit'. 
	once Int count() {
		runCmd(cmd
			.add("count", nsCol.collectionName)
			.add("query", compileQuery)
		)["n"]->toInt
	}

	** Returns 'true' if the cursor is alive on the server.
	** 
	** Note this returns 'false' if a query has not yet been sent to the server.
	Bool isAlive() {
		querySent.locked && cursorId != 0
	}

	** Returns a query plan that describes the process and indexes used to return the query. 
	** Useful when attempting to optimise queries.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/meta/explain/`
	Str:Obj? explain() {
		special["\$explain"] = 1
		return toList.first
	}
	
	// ---- Helper Methods ------------------------------------------------------------------------

	internal Void kill() {
		if (isAlive) {
			Operation(connection).killCursors([cursorId])
			cursorId = 0
			deadCursor.lock
		}
	}

	internal Void getSome() {
		// drain isn't passed in, 'cos it may be user set
		drain	:= flags.containsAll(OpQueryFlags.exhaust)

		deadCursor.check
		fields	:= (fieldNames == null) ? null : cmd.addList(fieldNames).map { 1 }
		
		// we want to limit the no. of returned results to the smallest, non-null value
		qlimit := [batchSize, limit].sort.find { it != null } ?: 0
		
		// if we're bringing down the entire limit, negate so the server doesn't keep the cursor 
		// open and exhaust *everything*!
		if (qlimit == limit)
			qlimit = -limit
		
		reply := Operation(connection).query(nsCol.qname, compileQuery, qlimit, skip, fields, flags)
		querySent.lock
		gotSome(reply, false)
		
		// if an 'exhaust' query then gulp down all the server replies
		if (drain) {
			while (isAlive) {
				reply = Operation(connection).readReply(null)
				gotSome(reply, true)
			}
		}
	}

	private Void getMore(Bool drain) {
		deadCursor.check

		// make sure the cursor doesn't ever bring down more than 'limit'
		qlimit := getMoreQlimit
		reply := Operation(connection).getMore(nsCol.qname, qlimit, cursorId)
		gotSome(reply, false)
		
		if (drain) {
			while (isAlive && (downloaded < maxDownload)) {
				reply = Operation(connection).getMore(nsCol.qname, getMoreQlimit, cursorId)
				gotSome(reply, true)
			}
			// and we're spent!
			kill
		}
	}

	private Void gotSome(OpReplyResponse reply, Bool append) {
		cursorId 	= reply.cursorId
		downloaded	+= reply.documents.size

		if (append) {
			results.addAll(reply.documents)
		} else {
			resultIndex	= 0			
			results 	= reply.documents
			if (index != (reply.cursorPos + skip))
				log.warn(LogMsgs.cursor_indexOutOfSync(index, reply.cursorPos + skip))
		}

		if (!isAlive)
			deadCursor.lock
	}
	
	private Int getMoreQlimit() {
		qlimit := batchSize ?: 0
		if (limit != null) {
			qlimit = limit - downloaded
		}
		return qlimit
	}
	
	private Int maxDownload() {
		limit ?: count - skip
	}
	
	private [Str:Obj?] compileQuery() {
		special.isEmpty ? query : cmd.add("\$query", query).addAll(special)
	}
	
	private Str:Obj? cmd() {
		Str:Obj?[:] { ordered = true }
	}	
	
	private Str:Obj? runCmd(Str:Obj? cmd) {
		Operation(connection).runCommand("${nsCol.databaseName}.\$cmd", cmd)
	}
}
