using concurrent::AtomicInt
using afBson::BsonIO

** 'MongoOpErrs' are thrown for any networking issues, which subsequently should invoke a Master
** failover in the Connection Manager.
** 
** @see `https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst`
@NoDoc	// advanced use only
internal class MongoOp {
	private static const AtomicInt	reqIdSeq	:= AtomicInt(0)

	private MongoConn connection

	** Creates an 'Operation' with the given connection.
	new make(MongoConn connection) {
		this.connection = connection
	}

	** Runs the given Mongo command and returns the reply document.
	Str:Obj? runCommand(Str dbName, Str:Obj? cmd, Bool checked := true) {
		if (cmd.ordered == false)
			throw ArgErr("Command Map is NOT ordered - this WILL (probably) result in a MongoDB error: ${dbName} -> ${cmd}")
		
		// this guy can NOT come first! Else, ERR, "Unknown Cmd $db"
		cmd["\$db"]	= dbName

		reqId	:= reqIdSeq.incrementAndGet
		out		:= connection.out
		out.endian	= Endian.little
		
		
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
		reqId	 = in.readU4	// should be the same
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
		
		if (checked && resDoc["ok"] != 1f) {
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

