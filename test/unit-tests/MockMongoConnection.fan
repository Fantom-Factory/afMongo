using afBson

internal class MockMongoConnection : Connection {
	override Str:Str	authentications	:= [:]
	Buf mongoIn			:= Buf()
	Buf mongoOut		:= Buf()
	BsonWriter replyOut	:= BsonWriter(mongoOut.out)
	
	override InStream in() {
		if (!mongoIn.isEmpty) {
			reqId := BsonReader(mongoIn.seek(4).in).readInteger32
//			if (!mongoOut.isEmpty)
			BsonWriter(mongoOut.seek(8).out).writeInteger32(reqId)
		}
		return mongoOut.seek(0).in
	}

	override OutStream	out()		{ mongoIn.out } 
	override Void		close()		{ }
	override Bool		isClosed()	{ false }
	
	Str:Obj? readSentDoc() {
		// unwind the msg
		reader := BsonReader(mongoIn.seek(0).in)
		reader.readInteger32
		reader.readInteger32
		reader.readInteger32
		reader.readInteger32
		
		reader.readInteger32
		reader.readCString
		reader.readInteger32
		reader.readInteger32
		return reader.readDocument
	}
	
	Void reply(Str:Obj? document) {
		replyOut.writeInteger32(-1)
		replyOut.writeInteger32(-1)
		replyOut.writeInteger32(42)
		replyOut.writeInteger32(OpCode.OP_REPLY.id)

		replyOut.writeInteger32(OpReplyFlags.none.value)
		replyOut.writeInteger64(0)
		replyOut.writeInteger32(0)
		replyOut.writeInteger32(1)
		replyOut.writeDocument(document)		
	}
	
	This reset() {
		mongoIn.clear
		mongoOut.clear
		return this
	}
}
