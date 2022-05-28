using afBson::BsonIO

internal class TestMsgCompression : Test {
	
	Void testCompressedReq() {
		// GIVEN
		con					:= MongoConnStub()
		con._compressor		= "zlib"
		con._zlibCompressionLevel	= 6
		con.writePreamble
		con.writeDoc(["judge":"anderson", "ok":1])
		con.flip
		
		// WHEN
		res := MongoOp(null, con, cmd("wotever").add("judge", "dredd")).runCommand("db")
		
		// THEN - req IS compressed
		in := con.outBuf.flip
		in.endian			= Endian.little
		in.readU4			// msg size
		in.readU4			// reqId
		in.readU4			// resId
		
		opCode := in.readU4
		verifyEq(opCode, 2012)
		
		opCode = in.readU4
		verifyEq(opCode, 2013)
		in.readU4			// uncompressed size
		compId := in.read
		verifyEq(compId, 2)	// zlib ID
		
		ins := Zip.deflateInStream(in.in)
		ins.readU4			// flag bits
		ins.read			// section ID
		req := BsonIO().readDoc(ins)
		
		verifyEq(req["judge"], "dredd")
		verifyEq(res["judge"], "anderson")
	}
	
	Void testCompressedRes() {
		// GIVEN
		con					:= MongoConnStub()
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(1)		// reqId
		con.writeI4(2012)	// opCode OP_COMPRESSED
		
		buf := Buf();		buf.endian	= Endian.little
		buf.writeI4(0)		// flagBits
		buf.write  (0)		// payloadType
		BsonIO().writeDoc(["judge":"anderson", "ok":1], buf)
		buf.flip
		
		zip := Buf();		zip.endian	= Endian.little
		Zip.deflateOutStream(zip.out).writeBuf(buf).flush.close
		zip.flip
		
		con.writeI4(2013)		// opCode OP_MSG
		con.writeI4(buf.size)	// uncompresses size
		con.writeI1(2)			// compressor ID
		con.writeBuf(zip)
		con.flip
		
		// WHEN
		res := MongoOp(null, con, cmd("wotever").add("judge", "dredd")).runCommand("db")
		
		// THEN - req is NOT compressed
		in := con.outBuf.flip
		in.endian			= Endian.little
		in.readU4			// msg size
		in.readU4			// reqId
		in.readU4			// resId
		
		opCode := in.readU4
		verifyEq(opCode, 2013)
		
		in.readU4			// flag bits
		in.read				// section ID
		req := BsonIO().readDoc(in.in)
		
		// check that both reqs and ress decode correctly
		verifyEq(req["judge"], "dredd")
		verifyEq(res["judge"], "anderson")
	}
	
	Void testOpsNotCompressed() {
		con					:= MongoConnStub()
		con._compressor		= "zlib"
		con.writeI4(0)		// msgSize
		con.writeI4(0)		// resId
		con.writeI4(1)		// reqId
		con.writeI4(2013)	// opCode
		con.writeI4(0)		// flagBits
		con.writeI1(0)		// payloadType
		con.writeDoc(["foo":"bar", "ok":1])
		con.flip
		
		res := MongoOp(null, con, cmd("isMaster")).runCommand("db")
		
		verifyEq(res["foo"], "bar")	
	}
	
	private [Str:Obj?] cmd(Str cmd) { Str:Obj?[:] { ordered = true }.add(cmd, 1) }
}
