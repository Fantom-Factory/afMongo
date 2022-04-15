
internal const class MongoErrMsgs {

	static Str opCode_unknownId(Int id) {
		"Could not find OpCode for id '${id}'"
	}
	
	static Str oneShotLock_violation(Str because) {
		"Method may no longer be invoked - $because"
	}	

	static Str operation_cmdNotOrdered(Str cmd, Str:Obj? doc) {
		"Command Map is NOT ordered - this will probably result in a MongoDB error: ${cmd} -> ${doc}"
	}
	
	static Str operation_resIdMismatch(Int reqId, Int resId) {
		"Response ID from MongoDB '${resId}' does not match Request ID '${reqId}'"
	}

	static Str operation_queryFailure(Str? errMsg) {
		"Query failed. MongoDB says: ${errMsg}"
	}
	
	static Str operation_invalid() {
		"MongoDB network issues detected"
	}
	
	static Str opReply_tooMany([Str:Obj?][] docs) {
		"Expected ONE document but OpReply has ${docs.size}! - ${docs}"
	}
	
	static Str opReply_isEmpty() {
		"Expected ONE document but OpReply has ZERO!"
	}
	
	static Str cursor_mapNotOrdered(Str:Obj? map) {
		"Maps with more than 1 entry must be ordered: ${map}"
	}
	
	static Str cursor_noMoreData() {
		"No more data"
	}
	
	static Str collection_findOneIsEmpty(Str qname, Obj? query) {
		"FindOne() query returned ZERO documents from '${qname}': $query"
	}

	static Str collection_findOneHasMany(Str qname, Int no, Str:Obj? query) {
		"FindOne() query returned $no documents from '${qname}': $query"
	}

	static Str collection_findAllSortArgBad(Obj sort) {
		stripSys("Sort argument must be either a Str (Cursor.hint) or a Map (Cursor.orderBy), not ${sort.typeof.signature} ${sort}")	
	}

	static Str collection_badKeyGroup(Obj key) {
		stripSys("Group key must be either a Str[] (field names) or a Str (function), not ${key.typeof.signature} ${key}")
	}

	static Str stripSys(Str str) {
		str.replace("sys::", "")
	}
}
