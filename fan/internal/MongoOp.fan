using concurrent::AtomicInt
using afBson::BsonIO

** Sends an 'OP_MSG' to the Mongo server.
** 
** @see 
**  - `https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst`
**  - `https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst`
**  - `https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst`
**  - `https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst`
**  - `https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst`
**  - `https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst`
@NoDoc	// advanced use only
internal class MongoOp {
	private static const Int		OP_COMPRESSED		:= 2012
	private static const Int		OP_MSG				:= 2013
	private static const AtomicInt	reqIdSeq			:= AtomicInt(0)
	private static const Str[]		reservedFields		:= "lsid txnNumber startTransaction autocommit".split
	private static const Str[]		uncompressibleCmds	:= "hello isMaster saslStart saslContinue getnonce authenticate createUser updateUser copydbSaslStart copydbgetnonce copydb".split
	private static const Str:Int	compressorIds		:= Str:Int[
		"noop"		: 0,
		"snappy"	: 1,
		"zlib"		: 2,
		"zstd"		: 3,
	]
	private static const Str[]		nonSessionCmds		:= "hello isMaster saslStart saslContinue getnonce authenticate".split
	private static const Int[]		retryableErrCodes	:= [
		11600,	// InterruptedAtShutdown
		11602,	// InterruptedDueToReplStateChange
		10107,	// NotWritablePrimary
		13435,	// NotPrimaryNoSecondaryOk
		13436,	// NotPrimaryOrSecondary
		  189,	// PrimarySteppedDown
		   91,	// ShutdownInProgress
		    7,	// HostNotFound
		    6,	// HostUnreachable
		   89,	// NetworkTimeout
		 9001,	// SocketException
		  262,	// ExceededTimeLimit
	]

	private Log				log
	private MongoConn		conn
	private Str:Obj?		cmd
	private Str				cmdName
	private MongoConnMgr?	connMgr
	private Bool			oneShotLock

	new make(MongoConnMgr? connMgr, MongoConn conn, Str:Obj? cmd) {
		if (cmd.ordered == false)
			throw ArgErr("Command Map is NOT ordered - this WILL (probably) result in a MongoDB error:\n${BsonIO().print(cmd)}")

		this.conn		= conn
		this.log		= conn.log
		this.cmd		= cmd.dup	// don't pollute the original cmd supplied by the user (this is a Mongo spec MUST)
		this.cmdName	= cmd.keys.first
		this.connMgr	= connMgr
	}

	Str:Obj? runCommand(Str dbName, Bool checked := true) {
		if (oneShotLock)
			throw Err("MongoOps can only be run once")
		oneShotLock = true
		
		badFields := reservedFields.intersection(cmd.keys)
		if (badFields.size > 0)
			throw Err("Cmd may not contain reserved field(s): " + badFields.join(", "))
		
		sess := conn._getSession(false)

		// this guy can NOT come first! Else, ERR, "Unknown Cmd $db"
		cmd["\$db"]	= dbName
		
		if (sess?.isInTxn == true)
			// not *every* cmd is allowed in txns, but I'll let the server decide what's valid and what's not
			sess.prepCmdForTxn(cmd)
		
		else
		// append session info where we should
		if (nonSessionCmds.contains(cmdName) == false && isUnacknowledgedWrite == false) {
			sess = conn._getSession(true)
			cmd["lsid"]	= sess.sessionId

			// txNumber is only applicable if in a session
			// add it now, so we keep the same txNumber between retries
			if (isRetryableWrite(sess))
				cmd["txnNumber"] = sess.newTxNum
		}
		
		try	{
			result := doRunCommand(sess, checked)
			sess?.postCmd(result)
			return result
		}
		catch	(IOErr ioe) {
			// mark ALL sessions as dirty regardless if the retry succeeds or not (as per spec)
			conn._getSession(false)?.markDirty
			
			if (isRetryableRead)
				return retryCommand(ioe, sess, checked)

			if (isRetryableWrite(sess))
				return retryCommand(ioe, sess, checked)
			
			// IOErrs have an implicit "UnknownTransactionCommitResult" label
			if (sess?.isInTxn == true && cmdName == "commitTransaction") {
				// configure writeConcern as per spec
				wc := cmd["writeConcern"] as Str:Obj? ?: Str:Obj?[:] { it.ordered = true }
				wc  = wc.rw
				cmd["writeConcern"] = wc.rw
				wc["w"] = "majority"
				if (wc["wtimeout"] == null)
					wc["wtimeout"] = 10000
				return retryCommand(ioe, sess, checked)
			}

			if (sess?.isInTxn == true && cmdName == "abortTransaction")
				try return retryCommand(ioe, sess, false)	// not checked
				catch return Str:Obj["ok":1f, "afMongo-abortErrMsg":ioe.toStr]

			throw ioe
		}
		
		catch	(MongoErr me) {
			// MMAPv1 storage does not support transactions - the spec is *very* strong that we deal with it like this!
			if (me.code == 20 && me.errMsg != null && me.errMsg.startsWith("Transaction numbers"))
				throw MongoErr("This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string.", me.errDoc, me)

			if (isRetryableRead && retryableErrCodes.contains(me.code ?: -1))
				return retryCommand(me, sess, checked)

			if ((isRetryableWrite(sess) && retryableErrCodes.contains(me.code ?: -1)) || me.errLabels.contains("RetryableWriteError"))
				return retryCommand(me, sess, checked)

			if (sess?.isInTxn == true && cmdName == "commitTransaction" && me.errLabels.contains("UnknownTransactionCommitResult")) {
				// configure writeConcern as per spec
				wc := cmd["writeConcern"] as Str:Obj? ?: Str:Obj?[:] { it.ordered = true }
				wc["w"] = "majority"
				if (wc["wtimeout"] == null)
					wc["wtimeout"] = 10000
				return retryCommand(me, sess, checked)
			}

			if (sess?.isInTxn == true && cmdName == "abortTransaction")
				try return retryCommand(me, sess, false)	// not checked
				catch return Str:Obj["ok":1f, "afMongo-abortErrMsg":me.toStr]

			throw me
		}
	}
	
	private Str:Obj? retryCommand(Err err, MongoSess? sess, Bool checked) {
		log.warn("Re-trying cmd '${cmdName}' - ${err.typeof} - ${err.msg} (${conn._mongoUrl})")

		try {
			conn.close
			
			// use get, so any failover errors are thrown 
			newMasterUrl := connMgr.failOver.get(10sec)	// the spec gives an example of 10sec

			// grab a fresh conn, 'cos the existing Conn just got closed!
			conn = conn._refresh(newMasterUrl)
			connMgr.authenticateConn(conn)

			return doRunCommand(sess, checked)

		} catch	(Err e) {
			conn.close

			log.warn("Re-try cmd failed - ${e.typeof} - ${e.msg} (${conn._mongoUrl})")

			// "If the retry attempt also fails, drivers MUST update their topology."
			connMgr.failOver
			
			// throw original error
			throw err
		}
	}
	
	private Str:Obj? doRunCommand(MongoSess? sess, Bool checked) {
		// the spec keep harping on about not re-sending the same clusterTime when re-trying Ops
		// so lets keep it fresh
		sess?.appendClusterTime(cmd)
		
		reqId	:= reqIdSeq.incrementAndGet
				writeRequest(reqId)
		return	readResponse(reqId, checked)
	}
	
	private Bool isRetryableWrite(MongoSess? sess) {
		if (connMgr == null || connMgr.mongoConnUrl.retryWrites == false)
			return false
		
		// "Transaction numbers are only allowed on a replica set member or mongos"
		if (connMgr.isStandalone)
			return false
		
		if (isUnacknowledgedWrite)
			return false
		
		if (sess?.isInTxn == true)			// transactions do NOT allow retryableWrties (the tx IS the retry!)
			return false

		// write commands that affect multiple documents are not supported
		// https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#supported-write-operations
		
		if (cmdName == "insert")
			return true
		
		if (cmdName == "update") {
			updates := cmd["updates"] as [Str:Obj?][]
			return updates != null && updates.all { it["multi"] == null || it["multi"] == false }
		}
		
		if (cmdName == "delete") {
			deletes := cmd["deletes"] as [Str:Obj?][]
			return deletes != null && deletes.all { it["limit"] == 1 }
		}

		if (cmdName == "findAndModify")
			return true
		
		return false
	}	

	private Bool isRetryableRead() {
		if (connMgr == null || connMgr.mongoConnUrl.retryReads == false)
			return false
		
		// https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst#id13
		
		// "getMore" is NOT allowed
		if (cmdName == "getMore")
			return false

		if ("find distinct count listDatabases listCollections listIndexes".split.contains(cmdName))
			return true
		
		if (cmdName == "aggregate") {
			pipeline := cmd["pipeline"] as [Str:Obj?][]
			return pipeline != null && pipeline.all { it.keys.first != "\$out" && it.keys.first != "\$merge" }
		}
		
		return false
	}
	
	private Bool isUnacknowledgedWrite() {
		(cmd["writeConcern"] as Str:Obj?)?.get("w") == 0	// { w: 0 }
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
		if (conn._compressor == "zlib" && uncompressibleCmds.contains(cmdName) == false) {

			compId := compressorIds[conn._compressor]
			zipBuf := Buf(msgBuf.size /2) ; zipBuf.endian	= Endian.little
			zipOpt := conn._zlibCompressionLevel == null ? null : Str:Obj?["level": conn._zlibCompressionLevel]
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
		msgId	:= in.readU4
		resTo	:= in.readU4
		opCode	:= in.readU4
	
		if (resTo != reqId) {
			// weirdly, in my Mongo 3.6 dev environment, Mongo looses sync and often starts sending out shite!
			help := conn._compressor != "gzip" ? "" : "  - if this Err persists, try disabling gzip compression"
			throw IOErr("Bad Mongo response, returned RequestID (${resTo}) does NOT match sent RequestID (${reqId})${help}")
		}
		
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
				throw IOErr("Unsupported compression algorithm: ${compId}" + (algo == null ? "" : " (${algo})"))
			}
		}
		
		if (opCode != OP_MSG)
			throw IOErr("Bad Mongo response, expected OP_MSG (${OP_MSG}) not: ${opCode}")
		
	
		// read OP_MSG
		flagBits	:= in.readU4
		if (flagBits != 0)
			throw IOErr("Bad Mongo response, expected NO flags, but got: 0x${flagBits.toHex}")
		
		payloadType	:= in.read
		if (payloadType != 0)
			throw IOErr("Bad Mongo response, expected payload type 0, not: ${payloadType}")

		resDoc	:= BsonIO().readDoc(in)
		
		if (log.isDebug) {
			msg := "Mongo Res ($msgId):\n"
			msg += BsonIO().print(resDoc)
			log.debug(msg)
		}
		
		conn._getSession(false)?.updateClusterTime(resDoc["\$clusterTime"])
		
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
				matcher2	:= "(_[a-zA-Z0-9]+_)".toRegex.matcher(errMsg)
				indexName	:= matcher2.find ? matcher2.group(1) : null
				matcher3	:= ": (\\{[^\\}]+\\})".toRegex.matcher(errMsg)
				keyValue	:= matcher3.find ? matcher3.group(1) : null

				if (collName != null && keyValue != null) {
					msg		:= "Command '${cmdName}' failed, index ${collName}.${indexName} ${keyValue} is already in use"
					mongoErr = MongoErr(msg, resDoc)
				}
			}
			throw mongoErr
		}

		return resDoc
	}
	
	** For testing.
	static internal Int reqId() {
		reqIdSeq.val
	}
}
