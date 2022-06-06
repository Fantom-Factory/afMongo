
internal class TestMongoOpStdComms : Test {
	
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
		
		res := MongoOp(null, con, cmd("wotever")).runCommand("<dbName>")
		
		verifyEq(res["foo"], "bar")
		
		// db SHOULD NOT pollute the given cmd
		verifyEq(res.containsKey("\$db"), false)
		
		// but it SHOULD have been sent to the server
		doc := con.readDoc
		verifyEq(doc["\$db"], "<dbName>")
	}
	
	Void testBadReqId() {
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(666666)	// reqId		- ###
		con.writeI4(2013)	// opCode
		con.writeI4(0)		// flagBits
		con.writeI1(0)		// payloadType
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		MongoOp#.field("reqIdSeq").get->val = 0	
		verifyErrMsg(IOErr#, "Bad Mongo response, returned RequestID (666666) does NOT match sent RequestID (1)") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
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
		
		verifyErrMsg(IOErr#, "Bad Mongo response, expected OP_MSG (2013) not: 69") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
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
		
		verifyErrMsg(IOErr#, "Bad Mongo response, expected NO flags, but got: 0x10") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
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
		
		verifyErrMsg(IOErr#, "Bad Mongo response, expected payload type 0, not: 1") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
		}
	}	

	Void testCmdFailed() {
		con	:= MongoConnStub().writePreamble
		con.writeDoc(["foo":"bar", "ok":0])
		con.flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
		}
	}
	

	Void testWriteErrors() {
		con	:= MongoConnStub().writePreamble
		con.writeDoc(["foo":"bar", "ok":1, "writeErrors":[,]])
		con.flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
		}
		
		con	= MongoConnStub().writePreamble
		con.writeDoc(["foo":"bar", "ok":1, "writeErrors":[["errmsg":"Booya!"]]])
		con.flip
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed. MongoDB says: Booya!") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
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
		
		verifyErrMsg(MongoErr#, "Command 'wotever' failed, index indexTest._data_ { data: 10 } is already in use") {
			MongoOp(null, con, cmd("wotever")).runCommand("wotever")
		}	
	}
	
	private [Str:Obj?] cmd(Str cmd) { Str:Obj?[:] { ordered = true }.add(cmd, 1) }
}
