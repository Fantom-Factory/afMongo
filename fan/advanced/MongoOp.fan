using concurrent::AtomicInt
using afBson::BsonIO

** Sends an 'OP_MSG' to the Mongo server.
** 
** @see `https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst`
@NoDoc	// advanced use only
class MongoOp {
	private static const AtomicInt	reqIdSeq	:= AtomicInt(0)

	private Log			log
	private MongoConn	conn

	new make(MongoConn conn) {
		this.conn	= conn
		this.log	= conn.log
	}

	Str:Obj? runCommand(Str dbName, Str:Obj? cmd, Bool checked := true) {
		if (cmd.ordered == false)
			throw ArgErr("Command Map is NOT ordered - this WILL (probably) result in a MongoDB error: ${dbName} -> ${cmd}")
		
		// this guy can NOT come first! Else, ERR, "Unknown Cmd $db"
		cmd["\$db"]	= dbName

		reqId	:= reqIdSeq.incrementAndGet
		out		:= conn.out
		out.endian	= Endian.little
		
		// TODO support compression
		// https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
		
		// TODO retryable writes
		// https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst
		
		if (log.isDebug) {
			msg := "Mongo Req ($reqId):\n"
			msg += BsonIO().print(cmd)
			log.debug(msg)
		}
		
		cmdBuf	:= BsonIO().writeDoc(cmd)	
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
		
		
		in			:= conn.in
		in.endian	= Endian.little
		
		// read std header
		msgSize	 = in.readU4
		reqId	 = in.readU4	// should be the same
		resId	:= in.readU4	// keep for logs
		opCode	:= in.readU4	// should be OP_MSG
		
		if (opCode != 2013)
			throw Err("Bad Mongo response, expected OP_MSG (2013), not: $opCode")
		
		flagBits	:= in.readU4
		if (flagBits != 0)
			throw Err("Bad Mongo response, expected NO flags, but got: ${flagBits.toHex}")
		
		payloadType	:= in.read
		if (payloadType != 0)
			throw Err("Bad Mongo response, expected payload type 0, not: $payloadType")

		resDoc	:= BsonIO().readDoc(in)
		
		if (log.isDebug) {
			msg := "Mongo Res ($resId):\n"
			msg += BsonIO().print(resDoc)
			log.debug(msg)
		}
		
		mongoErr	:= null as MongoErr
		if (checked && resDoc["ok"] != 1f && resDoc["ok"] != 1) {
			cmdName	:= cmd.keys.first
			errMsg  := resDoc["errmsg"] as Str
			msg		:= errMsg == null ? "Command '${cmdName}' failed" : "Command '${cmdName}' failed. MongoDB says: ${errMsg}"
			
			mongoErr = MongoErr(msg, resDoc)
		}
		
		if (checked && resDoc.containsKey("writeErrors")) {
			cmdName	:= cmd.keys.first
			wErrs	:= resDoc["writeErrors"] as [Str:Obj?][]	// yep - there can be many!
			errMsg  := wErrs.first.get("errmsg") as Str
			msg		:= errMsg == null ? "Command '${cmdName}' failed" : "Command '${cmdName}' failed. MongoDB says: ${errMsg}"
			mongoErr = MongoErr(msg, resDoc)
		}
		
		if (mongoErr != null) {
			// make a better err msg for duplicate index errors
			errMsg	:= mongoErr.errMsg
			if (errMsg != null && mongoErr.code == 11000) {
				matcher1	:= ".+_([a-zA-Z0-9]+)_.+".toRegex.matcher(errMsg)
				indexName	:= matcher1.find ? matcher1.group(1) : null
				matcher2	:= "\\{ : (.+?\\\")".toRegex.matcher(errMsg)
				keyValue	:= matcher2.find ? " ${matcher2.group(1)}" : null

				if (indexName != null && keyValue != null) {
					cmdName	:= cmd.keys.first
					msg		:= "Command '${cmdName}' failed, ${indexName}${keyValue} is already in use"
					mongoErr = MongoErr(msg, resDoc)
				}
			}
			throw mongoErr
		}

		return resDoc
	}
}

