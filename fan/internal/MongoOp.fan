using concurrent::AtomicInt
using afBson::BsonIO

** Sends an OP_MSG to the Mongo server.
** 
** @see `https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst`
@NoDoc	// advanced use only
class MongoOp {
	private static const AtomicInt	reqIdSeq	:= AtomicInt(0)

	private MongoConn connection

	new make(MongoConn connection) {
		this.connection = connection
	}

	Str:Obj? runCommand(Str dbName, Str:Obj? cmd, Bool checked := true) {
		if (cmd.ordered == false)
			throw ArgErr("Command Map is NOT ordered - this WILL (probably) result in a MongoDB error: ${dbName} -> ${cmd}")
		
		// this guy can NOT come first! Else, ERR, "Unknown Cmd $db"
		cmd["\$db"]	= dbName

		reqId	:= reqIdSeq.incrementAndGet
		out		:= connection.out
		out.endian	= Endian.little
		
		// TODO support compression
		// https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
		
		// TODO retryable writes
		// https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst
		
		echo("REQ: $reqId")
		BsonPrinter().print(cmd) { echo(it) }
		echo
		
		cmdBuf	:= BsonIO().writeDocument(cmd)	
		msgSize	:= cmdBuf.size
		
		// write std header
		out.writeI4(msgSize + 16 + 5)
		out.writeI4(reqId)
		out.writeI4(0)		// resId
		out.writeI4(2013)	// OP_MSG opCode

		// write OP_MSG
		out.writeI4(0)		// flagBits - why would I set *any* of them!?
		out.write(0)		// section payloadType == 0
		out.writeBuf(cmdBuf.flip)
		
		out.flush
		
		
		in		:= connection.in
		in.endian	= Endian.little
		
		// read std header
		msgSize	 = in.readU4
		reqId	 = in.readU4	// should be the same
		resId	:= in.readU4	// keep for logs
		opCode	:= in.readU4	// should be OP_MSG
		
		// TODO throw better Errs
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
		BsonPrinter().print(resDoc) { echo(it) }
		echo
		
		if (checked && resDoc["ok"] != 1f && resDoc["ok"] != 1) {
			errMsg  := resDoc["errmsg"] as Str
			msg		:= errMsg == null ? "Command '${cmd}' failed" : "Command '${cmd}' failed. MongoDB says: ${errMsg}"
			throw MongoErr(msg, resDoc)
		}
		
		return resDoc
	}
	
	static Str operation_resOpCodeInvalid(Int opCode) {
		"Response OpCode from MongoDB '${opCode}' should be : {OpCode.OP_REPLY.id} - {OpCode.OP_REPLY.name}"
	}
}
