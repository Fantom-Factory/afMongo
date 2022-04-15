using concurrent::AtomicInt
using afBson::BsonIO

** (Advanced)
** The low level transport mechanism that talks to MongoDB instances.
** 
** This is, actually, the only class you need to talk to a MongoDB instance!
** All other classes merely wrap calls to this. 
** 
** This class, and the BSON reader / writer classes, have been optimised for memory efficiency. 
** Feel free to send your 16Mb+ documents to MongoDB for they'll be streamed straight out over 
** the socket. 
** 
** 'MongoOpErrs' are thrown for any networking issues, which subsequently invoke a Master failover 
** in the Connection Manager.
** 
** @see `https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/`
class Operation {
	private static const Log log	:= Operation#.pod.log
	private static const AtomicInt	requestIdGenerator	:= AtomicInt(0)

	private Connection connection

	** Creates an 'Operation' with the given connection.
	new make(Connection connection) {
		this.connection = connection
	}

	** Runs the given Mongo command and returns the reply document.
	Str:Obj? runCommand(Str? dbName, Str:Obj? cmd, Bool checked := true) {
		if (cmd.size > 1 && !cmd.ordered)
			throw ArgErr(MongoErrMsgs.operation_cmdNotOrdered(dbName, cmd))
		
		if (dbName != null && dbName.contains("."))
			dbName = dbName.split('.').first
	
		// https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst
		
		reqId	:= requestIdGenerator.incrementAndGet
		out		:= connection.out
		out.endian	= Endian.little
		
		cmd		= cmd.rw
//		if (dbName != null)
		cmd["\$db"]	= dbName ?: "admin"
		// we could *also* set $readPreference here - {"mode":"primary"} 
		
		echo("REQ: $reqId")
		PrettyPrinter().print(cmd) { echo(it) }
		echo
		
		cmdBuf	:= BsonIO().writeDocument(cmd)	
		msgSize	:= cmdBuf.size
		
		// write std header
		out.writeI4(msgSize + 16 + 5)
		out.writeI4(reqId)
		out.writeI4(0)		// resId
		out.writeI4(2013)	// OP_MSG opCode
			
		// write OP_MSG
		out.writeI4(0)		// flagBits
		out.write(0)		// section payloadType == 0
		out.writeBuf(cmdBuf.flip)
		
		out.flush
		
		
		in		:= connection.in
		in.endian	= Endian.little
		
		// read std header
		msgSize	 = in.readU4
		reqId	= in.readU4		// should be the same
		resId	:= in.readU4	// keep for logs
		opCode	:= in.readU4	// should be OP_MSG
		
		if (opCode != 2013)
			throw Err("Wot not a OP_MSG!? $opCode")
		
		flagBits	:= in.readU4
		if (flagBits != 0)
			throw Err("Wot, got Flags!? ${flagBits.toHex}")
		
		payloadType	:= in.read
		if (payloadType != 0)
			throw Err("Wot, payload not type 0!? $payloadType")

		resDoc	:= BsonIO().readDocument(in)
		
		echo("RES: $resId")
		PrettyPrinter().print(resDoc) { echo(it) }
		echo
		
		if (checked && resDoc["ok"] != 1f)
			throw Err("Bad op")
//			throw MongoCmdErr(MongoErrMsgs.operation_cmdFailed(cname, doc["errmsg"] ?: doc), [doc])
		
		return resDoc

//	dbName := qname.split('.').first
//	doc := query(dbName, "RIBBIT", cmd, -1).document
//
////		doc := query(qname, cmd, -1).document
//
//		if (checked && (doc["ok"] != 1f && doc["ok"] != 1)) {
//			// attempt to work out the cmd, usually the first key in the given doc
//			cname := cmd.keys.first
//			throw MongoCmdErr(MongoErrMsgs.operation_cmdFailed(cname, doc["errmsg"] ?: doc), [doc])
//		}
//		return doc
	}
	
	static Str operation_cmdFailed(Str? cmd, Obj? errMsg) {
		"Command '${cmd}' failed. MongoDB says: ${errMsg}"
	}
	
	static Str operation_resOpCodeInvalid(Int opCode) {
		"Response OpCode from MongoDB '${opCode}' should be : {OpCode.OP_REPLY.id} - {OpCode.OP_REPLY.name}"
	}
}

