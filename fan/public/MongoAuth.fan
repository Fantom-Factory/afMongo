using util::Random

** Mongo Credentials used to authenticate connections.
** 
** See `https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst`.
@NoDoc	// advanced use only
const class MongoCreds {
	** Maybe one of:
	**  - 'SCRAM-SHA-1`
	const	Str			mechanism
	
	** The source database which the SASL commands will be sent to.
	const	Str			source
	
	** User credentials.
	const	Str?		username

	** User credentials.
	const	Str?		password
	
	** Any extra properties required by the authentication mechanisms.
	const	Str:Obj?	props

	** Default it-block ctor.
	new make(|This| fn) {
		fn(this)
		if (this.props == null)
			this.props = Str:Obj?[:]
	}
}

** See [Driver Authentication]`https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst`
@NoDoc	// advanced use only
const mixin MongoAuthMech {
	
	abstract Void authenticate(Connection conn, MongoCreds creds)
	
}

internal const class MongoAuthScramSha1 : MongoAuthMech {
	
	// Many thanks to the Erlang MongoDB driver for letting me decode their work!
	// https://github.com/alttagil/mongodb-erlang/blob/develop/src/core/mc_auth_logic.erl
	//
	// https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst
	// http://tools.ietf.org/html/rfc5802#page-7
	// http://tools.ietf.org/html/rfc4422#section-5
	// http://tools.ietf.org/html/rfc2898#section-5.2
	override Void authenticate(Connection conn, MongoCreds creds) {
		if (creds.mechanism != "SCRAM-SHA-1")
			throw UnsupportedErr("Only credentials for SCRAM-SHA-1 are supported")
		if (creds.username == null || creds.password == null)
			throw UnsupportedErr("Both username and password MUST be provided")
		
		gs2Header		:= "n,,"	// no idea where this comes from!
		
		// ---- 1st message ----
		random			:= Random.makeSecure
		clientNonce		:= Buf().writeI8(random.next).writeI8(random.next).toBase64
		clientFirstMsg	:= "n=${creds.username},r=${clientNonce}"
		serverFirstRes	:= MongoOp(conn).runCommand(creds.source, map
			.add("saslStart", 1)
			.add("mechanism", "SCRAM-SHA-1")
			.add("payload", Buf().print(gs2Header).print(clientFirstMsg))
			.add("autoAuthorize", 1)
		)
		
		conversationId	:=  (Int) serverFirstRes["conversationId"]
		serverFirstMsg	:= ((Buf) serverFirstRes["payload"]).readAllStr
		payloadValues	:= Str:Str[:].addList(serverFirstMsg.split(',')) { it[0..<1] }.map { it[2..-1] }
		serverNonce		:= payloadValues["r"]
		serverSalt		:= payloadValues["s"]
		serverIterations:= Int.fromStr(payloadValues["i"])

		// ---- 2nd message ----
		hashedPassword	:= "${creds.username}:mongo:${creds.password}".toBuf.toDigest("MD5").toHex
		dkLen			:= 20	// the size of a SHA-1 hash
		saltedPassword	:= Buf.pbk("PBKDF2WithHmacSHA1", hashedPassword, Buf.fromBase64(serverSalt), serverIterations, dkLen)
		clientFinalNoPf	:= "c=${gs2Header.toBuf.toBase64},r=${serverNonce}"
		authMessage		:= "${clientFirstMsg},${serverFirstMsg},${clientFinalNoPf}"
		clientKey		:= "Client Key".toBuf.hmac("SHA-1", saltedPassword)
		storedKey		:= clientKey.toDigest("SHA-1")
		clientSignature	:= authMessage.toBuf.hmac("SHA-1", storedKey)
		clientProof		:= xor(clientKey, clientSignature) 
		clientFinal		:= "${clientFinalNoPf},p=${clientProof.toBase64}"
		serverKey		:= "Server Key".toBuf.hmac("SHA-1", saltedPassword)
		serverSignature	:= authMessage.toBuf.hmac("SHA-1", serverKey).toBase64
		serverSecondRes := MongoOp(conn).runCommand(creds.source, map
			.add("saslContinue", 1)
			.add("conversationId", conversationId)
			.add("payload", Buf().print(clientFinal))
		)
		serverSecondMsg	:= ((Buf) serverSecondRes["payload"]).readAllStr
		payloadValues	= Str:Str[:].addList(serverSecondMsg.split(',')) { it[0..<1] }.map { it[2..-1] }
		serverProof		:= payloadValues["v"]

		// authenticate the server
		if (serverSignature != serverProof)
			throw Err("Mongo Server sent invalid SCRAM signature '${serverSignature}' - was expecting '${serverProof}'")

		// ---- 3rd message ----
		serverThirdRes := MongoOp(conn).runCommand(creds.source, map
			.add("saslContinue", 1)
			.add("conversationId", conversationId)
			.add("payload", Buf())
		)
		if (serverThirdRes["done"] != true)
			throw Err("Mongo SCRAM authentication did not complete - ${serverThirdRes}")
	}
	
	private Buf xor(Buf key, Buf sig) {
		out := Buf()
		for (i := 0; i < key.size; ++i) {
			out.write(key.read.xor(sig.read))
		}
		return out.flip
	}
	
	private [Str:Obj?] map() { Str:Obj?[:] { ordered = true } }
}