using afBson::BsonIO
using concurrent::ActorPool

internal class MongoConnStub : MongoConn {
	override Log		log				:= typeof.pod.log
	override Bool		isClosed		:= false
	override Bool		isAuthenticated	:= false	
	override Str?		compressor
	override Int?		zlibCompressionLevel
	private	 MongoSessPool	sessPool	:= MongoSessPool(ActorPool())
	private	 MongoSess?		sess

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

	override InStream 	in()	{ inBuf.in }
	override OutStream	out()	{ outBuf.out }
	override Void		close()	{ }
	
	override MongoSess? getSession(Bool createNew) {
		if (sess != null)
			return sess
		
		if (createNew == false)
			return null

		return sess = sessPool.checkout
	}

	override MongoSess? detachSession() {
		sess := this.sess
		this.sess = null
		if (sess != null)
			sess.isDetached = true
		return sess
	}
	
	override Void setSession(MongoSess? session) {
		if (session == null) return

		if (this.sess != null)
			throw Err("Cannot setSession(), I've already got one - $sess")

		if (session.isDetached == false)
			throw Err("Cannot setSession(), Session is NOT detached - $sess")

		this.sess = session
	}
	
	Str:Obj? readDoc() {
		// THEN - req is NOT compressed
		in := outBuf.flip.in
		in.endian			= Endian.little
		in.readU4			// msg size
		in.readU4			// reqId
		in.readU4			// resId
		opCode := in.readU4
		
		if (opCode == 2012) {
			opCode = in.readU4
			in.readU4			// uncompressed size
			compId := in.read	// zlib ID
			if (compId == 2)
				in = Zip.deflateInStream(in)
		}
		
		in.readU4			// flag bits
		in.read				// section ID
		return BsonIO().readDoc(in)
	}
}
