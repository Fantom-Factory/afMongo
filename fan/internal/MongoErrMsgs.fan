
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
	
	static Str operation_resOpCodeInvalid(Int opCode) {
		"Response OpCode from MongoDB '${opCode}' should be : ${OpCode.OP_REPLY.id} - ${OpCode.OP_REPLY.name}"
	}

	static Str operation_queryFailure(Str? errMsg) {
		"Query failed. MongoDB says: ${errMsg}"
	}
	
	static Str operation_resInvalid() {
		"Network issues reading response; dodgy MongoDB Atlas infrastructure suspected"
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
	
	static Str cursor_mapNotOrdered(Str:Obj? map) {
		"Maps with more than 1 entry must be ordered: ${map}"
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

	static Str cmd_writeErrs(Str what, Str colName, [Str:Obj?][] errs) {
		"Errors ${what} '${colName}' - " + ((errs.size == 1) ? errs.first["errmsg"].toStr : errs.toStr)
	}
	
	static Str connectionManager_badScheme(Uri mongoUrl) {
		"Mongo connection URIs must start with the scheme 'mongodb://' - ${mongoUrl}"
	}
	
	static Str connectionManager_badUsernamePasswordCombo(Str? username, Str? password, Uri mongoUrl) {
		"Either both the username and password should be provided, or neither. username=$username, password=$password, url=$mongoUrl"
	}
	
	static Str connectionManager_badInt(Str what, Str min, Int val, Uri mongoUrl) {
		"$what must be greater than $min! val=$val, uri=$mongoUrl"
	}
	
	static Str connectionManager_badMinMaxConnectionSize(Int min, Int max, Uri mongoUrl) {
		"Minimum number of connections must not be greater than the maximum! min=$min, max=$max, url=$mongoUrl"
	}
	
	static Str connectionManager_notStarted() {
		"ConnectionManager has not started"
	}
	
	static Str connectionManager_couldNotFindPrimary(Uri mongoUrl) {
		"Could not find the primary node with RelicaSet connection URL ${mongoUrl}"
	}
	
	static Str connectionManager_noConnectionInThread() {
		"No connection is available in this thread!?"
	}
	
	static Str connection_couldNot(Str ipAddr, Int port, Str errMsg) {
		"Could not connect to MongoDB at `${ipAddr}:${port}` - ${errMsg}"
	}
	
	static Str connection_unknownAuthMechanism(Str mechanism, Str[] supportedMechanisms) {
		"Unknown authentication mechanism '${mechanism}', only the following are currently supported: " + supportedMechanisms.join(", ")
	}
	
	static Str connection_invalidServerSignature(Str client, Str server) {
		"Server sent invalid SCRAM signature '${server}' - was expecting '${client}'"
	}
	
	static Str connection_scramNotDone(Str serverResponse) {
		"SCRAM authentication did not complete - ${serverResponse}"
	}
	
	static Str stripSys(Str str) {
		str.replace("sys::", "")
	}
}
