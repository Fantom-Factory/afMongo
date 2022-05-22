using concurrent::AtomicInt
using afBson::BsonIO

** Sends an 'OP_MSG' to the Mongo server.
** 
** @see `https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst`
** @see `https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst`
@NoDoc	// advanced use only
class MongoOp {
	private static const Int		OP_COMPRESSED		:= 2012
	private static const Int		OP_MSG				:= 2013
	private static const AtomicInt	reqIdSeq			:= AtomicInt(0)
	private static const Str[]		uncompressibleCmds	:= "hello isMaster saslStart saslContinue getnonce authenticate createUser updateUser copydbSaslStart copydbgetnonce copydb".split
	private static const Str:Int	compressorIds		:= Str:Int[
		"noop"		: 0,
		"snappy"	: 1,
		"zlib"		: 2,
		"zstd"		: 3,
	]
	private static const Str[]		nonSessionCmds		:= "hello isMaster saslStart saslContinue getnonce authenticate".split

	private Log			log
	private MongoConn	conn
	private Str:Obj?	cmd
	private Str			cmdName

	new make(MongoConn conn, Str:Obj? cmd) {
		if (cmd.ordered == false)
			throw ArgErr("Command Map is NOT ordered - this WILL (probably) result in a MongoDB error:\n${BsonIO().print(cmd)}")

		this.conn		= conn
		this.log		= conn.log
		this.cmd		= cmd.dup	// don't pollute the original cmd supplied by the user (this is a Mongo spec MUST)
		this.cmdName	= cmd.keys.first
	}

	Str:Obj? runCommand(Str dbName, Bool checked := true) {
		// this guy can NOT come first! Else, ERR, "Unknown Cmd $db"
		cmd["\$db"]	= dbName
		
		isUnacknowledgedWrite := (cmd["writeConcern"] as Str:Obj?)?.get("w") == 0	// { w: 0 }

		// append session info where we should
		if (nonSessionCmds.contains(cmdName) == false && isUnacknowledgedWrite == false)
			cmd["lsid"]	= conn.getSession.sessionId
		
		// TODO gossip clustertime


		// TODO retryable writes
		// https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst
		// Write commands specifying an unacknowledged write concern (e.g. {w: 0})) do not support retryable behavior.
		
		// For server versions 4.4 and newer, the server will add a RetryableWriteError label to errors or server responses that it considers retryable 
		
		
		// Generate session IDs ourselves! NO SERVER CALL NEEDED! good - 'cos it's not part of stable API
		// https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#generating-a-session-id-locally
		// https://www.rfc-editor.org/rfc/rfc4122#page-14
		
		// TODO retryable reads
		// https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst

		reqId	:= reqIdSeq.incrementAndGet
				writeRequest(reqId)
		return	readResponse(reqId, checked)
	}

	private Void writeRequest(Int reqId) {
		
		if (log.isDebug) {
			msg := "Mongo Req ($reqId):\n"
			msg += BsonIO().print(cmd)
			log.debug(msg)
		}
		
		msgBuf := Buf()
		msgOut := msgBuf.out;	msgOut.endian = Endian.little
		conOut := conn.out;		conOut.endian = Endian.little

		// write OP_MSG
		msgOut.writeI4(0)	// flagBits - why would I set *any* of these!?
		msgOut.write(0)		// section payloadType == 0
		BsonIO().writeDoc(cmd, msgBuf)
		msgBuf.flip
	
		// compress the msg if we're able
		if (conn.compressor == "zlib" && uncompressibleCmds.contains(cmdName) == false) {

			compId := compressorIds[conn.compressor]
			zipBuf := Buf(msgBuf.size /2) ; zipBuf.endian	= Endian.little
			zipOpt := conn.zlibCompressionLevel == null ? null : Str:Obj?["level": conn.zlibCompressionLevel]
			Zip.deflateOutStream(zipBuf.out, zipOpt).writeBuf(msgBuf).flush.close
			zipBuf.flip

			// write std MsgHeader
			conOut.writeI4(16 + 9 + zipBuf.size)
			conOut.writeI4(reqId)
			conOut.writeI4(0)				// resId
			conOut.writeI4(OP_COMPRESSED)	// OP_COMPRESSED opCode
			
			conOut.writeI4(OP_MSG)		// OP_MSG opCode
			conOut.writeI4(msgBuf.size)	// uncompresses size
			conOut.write(compId)		// Compressor ID
			conOut.writeBuf(zipBuf)
		}
		
		else {
			
			// write std MsgHeader
			conOut.writeI4(16 + msgBuf.size)
			conOut.writeI4(reqId)
			conOut.writeI4(0)			// resId
			conOut.writeI4(OP_MSG)		// OP_MSG opCode

			conOut.writeBuf(msgBuf)
		}
		
		conOut.flush
	}
	
	private Str:Obj? readResponse(Int reqId, Bool checked) {
		in			:= conn.in
		in.endian	= Endian.little
		
		// read std MsgHeader
		msgSize	:= in.readU4
		resId	:= in.readU4		// keep for logs
		reqId2	:= in.readU4
		opCode	:= in.readU4
	
		if (reqId2 != reqId)
			throw Err("Bad Mongo response, returned RequestID (${reqId2}) does NOT match sent RequestID (${reqId})")
		
		if (opCode == OP_COMPRESSED) {
			opCode	 = in.readU4	// original opCode
			unSize	:= in.readU4	// uncompressed size
			compId	:= in.read		// compressor ID
			
			if (compId == compressorIds["zlib"])
				in = Zip.deflateInStream(in)
			
			else
			if (compId == compressorIds["noop"])
				{ /* noop */ }
			
			else {
				algo := compressorIds.eachWhile |id, algo| { id == compId ? algo : null }
				// we don't throw UnsupportedErr because we *should* have negotiated a valid compressor
				// so this is an actual error
				throw Err("Unsupported compression algorithm: ${compId}" + (algo == null ? "" : " (${algo})"))
			}
		}
		
		if (opCode != OP_MSG)
			throw Err("Bad Mongo response, expected OP_MSG (${OP_MSG}), not: ${opCode}")
		
		
		// read OP_MSG
		flagBits	:= in.readU4
		if (flagBits != 0)
			throw Err("Bad Mongo response, expected NO flags, but got: 0x${flagBits.toHex}")
		
		payloadType	:= in.read
		if (payloadType != 0)
			throw Err("Bad Mongo response, expected payload type 0, not: ${payloadType}")

		resDoc	:= BsonIO().readDoc(in)
		
		if (log.isDebug) {
			msg := "Mongo Res ($resId):\n"
			msg += BsonIO().print(resDoc)
			log.debug(msg)
		}
		
		mongoErr	:= null as MongoErr
		if (checked && resDoc["ok"] != 1f && resDoc["ok"] != 1) {
			errMsg  := resDoc["errmsg"] as Str
			msg		:= errMsg == null ? "Command '${cmdName}' failed" : "Command '${cmdName}' failed. MongoDB says: ${errMsg}"
			
			mongoErr = MongoErr(msg, resDoc)
		}
		
		if (checked && resDoc.containsKey("writeErrors")) {
			wErrs	:= resDoc["writeErrors"] as [Str:Obj?][]	// yep - there can be many!
			errMsg  := wErrs?.first?.get("errmsg") as Str
			msg		:= errMsg == null ? "Command '${cmdName}' failed" : "Command '${cmdName}' failed. MongoDB says: ${errMsg}"
			mongoErr = MongoErr(msg, resDoc)
		}
		
		if (mongoErr != null) {
			// make a better err msg for duplicate index errors
			errMsg	:= mongoErr.errMsg
			if (errMsg != null && mongoErr.code == 11000) {
				// E11000 duplicate key error collection: afMongoTest.indexTest index: _data_ dup key: { data: 10 }
				matcher1	:= "\\.([a-zA-Z0-9]+) index".toRegex.matcher(errMsg)
				collName	:= matcher1.find ? matcher1.group(1) : null
				matcher2	:= ": (\\{[^\\}]+\\})".toRegex.matcher(errMsg)
				keyValue	:= matcher2.find ? matcher2.group(1) : null

				if (collName != null && keyValue != null) {
					msg		:= "Command '${cmdName}' failed, IndexKey ${collName} ${keyValue} is already in use"
					mongoErr = MongoErr(msg, resDoc)
				}
			}
			throw mongoErr
		}

		return resDoc
	}
	
	** For testing.
	static internal Void resetReqIdSeq() {
		reqIdSeq.val = 0
	}
}

