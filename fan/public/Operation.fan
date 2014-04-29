using concurrent
using afBson
using inet

** The low level transport mechanism that talks to MongoDB instances.
** 
** This is, actually, the only class you need to talk to a MongoDB instance!
** All other classes merely wrap calls to this. 
** 
** This class, and the BSON reader / writer classes, have been optomised for memory efficiency over 
** speed. Feel free to send your 16Mb+ documents to MongoDB for they'll be streamed straight out 
** over the socket. 
** 
** @see `http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol`
class Operation {
	private static const Log log	:= Utils.getLog(Operation#)
	private static const AtomicInt	requestIdGenerator	:= AtomicInt(0)

	private Connection connection

	** Creates an 'Operation' with the given connection.
	new make(Connection connection) {
		this.connection = connection
	}

	** Runs the given Mongo command and returns the reply document.
	Str:Obj? runCommand(Str qname, Str:Obj? cmd) {
		if (cmd.size > 1 && !cmd.ordered)
			throw ArgErr(ErrMsgs.operation_cmdNotOrdered(qname, cmd))
		
		doc := query(qname, cmd, -1).document

		if (doc["ok"] != 1f && doc["ok"] != 1) {
			// attempt to work out the cmd, usually the first key in the given doc
			cname := cmd.keys.first
			throw MongoCmdErr(ErrMsgs.operation_cmdFailed(cname, doc["errmsg"] ?: doc))
		}
		return doc
	}

	** Use to query MongoDB for documents in a collection.
	** 
	** @see `http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query`
	OpReplyResponse query(Str qname, Str:Obj? query, Int limit := 0, Int skip := 0, [Str:Obj?]? fields := null, Flag flags := OpQueryFlags.none) {
		sizer	:= BsonWriter(null)
		msgSize	:= 4 + sizer.sizeCString(qname) + 4 + 4 + sizer.sizeDocument(query) + sizer.sizeDocument(fields)
		reqId 	:= sendMsg(OpCode.OP_QUERY, msgSize) |out| {
			out.writeInteger32(flags.value)
			out.writeCString(qname)
			out.writeInteger32(skip)
			out.writeInteger32(limit)
			out.writeDocument(query)
			out.writeDocument(fields)
		}		
		return readReply(reqId)
	}

	** Use to ask MongoDB for more documents from a query.
	** 
	** @see `http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-get-more`
	OpReplyResponse getMore(Str qname, Int limit, Int cursorId) {
		sizer	:= BsonWriter(null)
		msgSize	:= 4 + sizer.sizeCString(qname) + 4 + 8
		reqId 	:= sendMsg(OpCode.OP_GET_MORE, msgSize) |out| {
			out.writeInteger32(0)
			out.writeCString(qname)
			out.writeInteger32(limit)
			out.writeInteger64(cursorId)
		}
		return readReply(reqId)
	}

	** Use to close an active cursor in the database.
	** 
	** @see `http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors`
	Void killCursors(Int[] cursorIds) {
		msgSize	:= 4 + 4 + (cursorIds.size * 8)
		sendMsg(OpCode.OP_KILL_CURSORS, msgSize) |out| {
			out.writeInteger32(0)
			out.writeInteger32(cursorIds.size)
			cursorIds.each { out.writeInteger64(it) }			
		}
	}
	
	// ---- Protected Methods ----
	
	** 'msgSize' and 'outFunc' ensure we can stream the entire msg straight out to the MongoDB
	** without the use of 'Buf()'. Given people tend to save 20Mb Objects in Mongo, this is a good 
	** thing!
	@NoDoc
	protected Int sendMsg(OpCode opCode, Int msgSize, |BsonWriter| outFunc) {
		requestId 	:= requestIdGenerator.incrementAndGet
		out 		:= BsonWriter(connection.out)
		
		// write std header
		out.writeInteger32(msgSize + 16)
		out.writeInteger32(requestId)
		out.writeInteger32(0)
		out.writeInteger32(opCode.id)
		
		// write msg
		outFunc.call(out)
		out.flush
		
		return requestId
	}

	** Reads a reply from the server.
	** 
	** 'requestId' may be 'null' when gulping down replies resulting from an *exhaust* query. 
	OpReplyResponse readReply(Int? requestId) {
		in 		:= BsonReader(connection.in)
		
		// read std header
		msgSize	:= in.readInteger32	// we ignore this and let the BsonReader check the size of the documents instead 
		reqId	:= in.readInteger32	// we ignore this
		resId	:= in.readInteger32
		opCode	:= in.readInteger32
		
		if (opCode != OpCode.OP_REPLY.id)
			throw MongoOpErr(ErrMsgs.operation_resOpCodeInvalid(opCode))
		if (requestId != null && requestId != resId)
			throw MongoOpErr(ErrMsgs.operation_resIdMismatch(requestId, resId))
    
		resFlags	:= OpReplyFlags(in.readInteger32)
		cursorId	:= in.readInteger64
		cursorPos	:= in.readInteger32
		noOfDocs	:= in.readInteger32		
		documents	:= [Str:Obj?][,] 

		noOfDocs.times {
			documents.add(in.readDocument)			
		}

		if (resFlags.containsAll(OpReplyFlags.queryFailure)) {
			// $err may not be a Str!
			// see http://docs.mongodb.org/meta-driver/latest/legacy/error-handling-in-drivers/
			errMsg := documents.first?.get("\$err")?.toStr
			throw MongoOpErr(ErrMsgs.operation_queryFailure(errMsg))
		}
			
		return OpReplyResponse {
			it.cursorId	 	= cursorId
			it.cursorPos	= cursorPos
			it.flags	 	= resFlags
			it.documents	= documents
		}
	}	
}

