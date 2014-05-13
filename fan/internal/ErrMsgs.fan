
internal const mixin ErrMsgs {

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
	
	static Str operation_resOpCodeInvalid(Int opCode) {
		"Response OpCode from MongoDB '${opCode}' should be : ${OpCode.OP_REPLY.id} - ${OpCode.OP_REPLY.name}"
	}

	static Str operation_queryFailure(Str? errMsg) {
		"Query failed. MongoDB says: ${errMsg}"
	}
	
	static Str operation_cmdFailed(Str? cmd, Obj? errMsg) {
		"Command '${cmd}' failed. MongoDB says: ${errMsg}"
	}
	
	static Str opReply_tooMany([Str:Obj?][] docs) {
		"Expected ONE document but OpReply has ${docs.size}! - ${docs}"
	}
	
	static Str opReply_isEmpty() {
		"Expected ONE document but OpReply has ZERO!"
	}
	
	static Str cursor_noMoreData() {
		"No more data"
	}
	
	static Str namespace_nameCanNotBeEmpty(Str what) {
		"${what} name can not be empty"
	}
	
	static Str namespace_nameTooLong(Str what, Str name, Int maxSize) {
		"${what} name must be shorter than ${maxSize} bytes: ${name}"
	}
	
	static Str namespace_nameHasInvalidChars(Str what, Str name, Str invalidChars) {
		"${what} name '${name}' may not contain any of the following: ${invalidChars}"
	}
		
	static Str collection_findOneIsEmpty(Str qname, Str:Obj? query) {
		"FindOne() query returned ZERO documents from '${qname}': $query"
	}

	static Str collection_findOneHasMany(Str qname, Int no, Str:Obj? query) {
		"FindOne() query returned $no documents from '${qname}': $query"
	}

	static Str collection_writeErrs(Str what, Str colName, [Str:Obj?][] errs) {
		"Errors ${what} '${colName}' - " + ((errs.size == 1) ? errs.first.toStr : errs.toStr)
	}
	
	static Str collection_nothingHappened(Str what, [Str:Obj?] response) {
		"Nothing ${what}! ${response}"
	}
	
//	static Str collection_nameReserved(Str name) {
//		"Collection names beginning with 'system.' are reserved: ${name}"
//	}
	
}
