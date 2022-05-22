using afBson::BsonIO

internal class TestMongoOp : Test {
	
	Void testStdComms() {
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(1)		// reqId
		con.writeI4(2013)	// opCode
		con.writeI4(0)		// flagBits
		con.writeI1(0)		// payloadType
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		res := MongoOp(con, cmd("wotever")).runCommand("wotever")
		
		verifyEq(res["foo"], "bar")
	}
	
	Void testBadReqId() {
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(2)		// reqId		- ###
		con.writeI4(2013)	// opCode
		con.writeI4(0)		// flagBits
		con.writeI1(0)		// payloadType
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		verifyErrMsg(Err#, "Bad Mongo response, returned RequestID (2) does NOT match sent RequestID (1)") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
	}	

	Void testBadOpCode() {
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(1)		// reqId
		con.writeI4(69)		// opCode		- ###
		con.writeI4(0)		// flagBits
		con.writeI1(0)		// payloadType
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		verifyErrMsg(Err#, "Bad Mongo response, expected OP_MSG (2013), not: 69") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
	}

	Void testBadFlagBits() {
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(1)		// reqId
		con.writeI4(2013)	// opCode
		con.writeI4(0x10)	// flagBits		- ###
		con.writeI1(0)		// payloadType
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		verifyErrMsg(Err#, "Bad Mongo response, expected NO flags, but got: 0x10") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
	}
	
	Void testBadPayloadType() {
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(1)		// reqId
		con.writeI4(2013)	// opCode
		con.writeI4(0)		// flagBits
		con.writeI1(1)		// payloadType	- ###
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		verifyErrMsg(Err#, "Bad Mongo response, expected payload type 0, not: 1") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
	}	

	Void testCmdFailed() {
		con	:= MongoConnStub().writePreamble
		con.writeDoc(["foo":"bar", "ok":0])
		con.flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
	}
	

	Void testWriteErrors() {
		con	:= MongoConnStub().writePreamble
		con.writeDoc(["foo":"bar", "ok":1, "writeErrors":[,]])
		con.flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
		
		con	= MongoConnStub().writePreamble
		con.writeDoc(["foo":"bar", "ok":1, "writeErrors":[["errmsg":"Booya!"]]])
		con.flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed. MongoDB says: Booya!") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}
	}
	
	Void testIndexErrors() {
		con	:= MongoConnStub().writePreamble
		con.writeDoc([
			"n"				: 0,
			"writeErrors"	: [[
				"index"		: 0,
				"code"		: 11000,
				"keyPattern": ["data": 1],
				"keyValue"	: ["data", 10],
				"errmsg"	: "E11000 duplicate key error collection: afMongoTest.indexTest index: _data_ dup key: { data: 10 }",
			]],
			"ok"			: 1.0f,
		]).flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed, IndexKey indexTest { data: 10 } is already in use") {
			MongoOp(con, cmd("wotever")).runCommand("wotever")
		}	
	}
	
	private [Str:Obj?] cmd(Str cmd) { Str:Obj?[:] { ordered = true }.add(cmd, 1) }
}

internal class MongoConnStub : MongoConn {
	override Log		log				:= typeof.pod.log
	override Bool		isClosed		:= false
	override Bool		isAuthenticated	:= false	
	override Str?		compressor
	override Int?		zlibCompressionLevel

	Buf	inBuf	:= Buf() { it.endian = Endian.little }
	Buf outBuf	:= Buf() { it.endian = Endian.little }
	
	new make() {
		MongoOp.resetReqIdSeq
	}
	
	This writeI1(Int i1)		{ inBuf.write(i1);					return this }
	This writeI4(Int i4)		{ inBuf.writeI4(i4);				return this }
	This writeBuf(Buf buf)		{ inBuf.writeBuf(buf);				return this }
	This writeDoc(Str:Obj? doc)	{ BsonIO().writeDoc(doc, inBuf);	return this }
	This flip()					{ inBuf.flip;						return this }
	This writePreamble()		{
		writeI4(0)		// msgSize
		writeI4(0)		// resId
		writeI4(1)		// reqId
		writeI4(2013)	// opCode
		writeI4(0)		// flagBits
		writeI1(0)		// payloadType
		return this
	}

	override MongoSess	getSession()					{ throw UnsupportedErr() }
	override MongoSess?	detachSession()					{ throw UnsupportedErr() }
	override Void		setSession(MongoSess? session)	{ throw UnsupportedErr() }

	override InStream 	in()	{ inBuf.in }
	override OutStream	out()	{ outBuf.out }
	override Void		close()	{ }
}
