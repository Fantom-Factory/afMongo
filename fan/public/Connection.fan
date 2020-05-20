using inet::IpAddr
using inet::TcpSocket
using util::Random

** Represents a connection to a MongoDB instance.
** All connections on creation should be connected to a MongoDB instance and ready to go.
** 
** Default implementation is a 'TcpConnection'. 
mixin Connection {

	** Data *from* MongoDB *to* the client.
	abstract InStream 	in()
	
	** Data *to* MongoDB *from* the client.
	abstract OutStream	out()
	
	** Closes the connection.
	** Should never throw an IOErr.
	abstract Void		close()	
	
	** Return 'true' if this socket is closed. 
	abstract Bool		isClosed()
	
	** A map of databases -> users this connection is authenticated as. 
	abstract Str:Str	authentications
	
	** Authenticates this connection against a database with the given user credentials. 
	** If given, 'mechanism' must be one of:
	**  - 'SCRAM-SHA-1'   - default for MongoDB 3.x +
	**  - 'MONGODB-CR'    - default for MongoDB 2.x
	abstract Void authenticate(Str databaseName, Str userName, Str password, Str? mechanism := null)
	
	** Logs this connection out from the given database.
	abstract Void logout(Str databaseName, Bool checked := true)
}

** Connects to MongoDB via an 'inet::TcpSocket'.
@NoDoc
class TcpConnection : Connection {
			 TcpSocket	socket
			 Version?	mongoDbVer
			 Uri?		mongoUrl
			 Bool		forceCloseOnCheckIn
	override Str:Str	authentications	:= [:]
	
	** Allows you to pass in a TcpSocket with options already set.
	new fromSocket(TcpSocket socket) {
		this.socket = socket
	}

	** Creates a new TCP Socket
	new make(Bool ssl) {
		this.socket = ssl ? TcpSocket.makeTls : TcpSocket.make
	}
	
	This connect(IpAddr address := IpAddr("127.0.0.1"), Int port := 27017) {
		try {
			socket.connect(address, port)
			return this
		}
		catch (Err err)
			throw IOErr(ErrMsgs.connection_couldNot(address.toStr, port, err.msg))		
	}
	
	override InStream	in()		{ socket.in			}
	override OutStream	out()		{ socket.out		}
	override Void		close()		{ socket.close		}
	override Bool		isClosed()	{ socket.isClosed	}
	
	override Void authenticate(Str databaseName, Str userName, Str password, Str? mechanism := null) {
		if (mechanism != null && mechanism != "SCRAM-SHA-1" && mechanism != "MONGODB-CR")
			throw ArgErr(ErrMsgs.connection_unknownAuthMechanism(mechanism, ["SCRAM-SHA-1", "MONGODB-CR"]))
		
		// https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst#determining-server-version
		if (mongoDbVer == null)
			mongoDbVer = Version.fromStr(Operation(this).runCommand("admin.\$cmd", ["buildInfo": 1])["version"], true)
		
		// set default auth mechanisms
		if (mechanism == null)
			mechanism = mongoDbVer >= Version([3,0,0]) ? "SCRAM-SHA-1" : "MONGODB-CR"
		
		if (mechanism == "MONGODB-CR")
			authMongoDbCr(databaseName, userName, password)
		if (mechanism == "SCRAM-SHA-1")
			authScramSha1(databaseName, userName, password)
		
		authentications[databaseName] = userName
	}
	
	// Many thanks to the Erlang MongoDB driver for letting me decode their work!
	// https://github.com/alttagil/mongodb-erlang/blob/develop/src/core/mc_auth_logic.erl
	//
	// https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst
	// http://tools.ietf.org/html/rfc5802#page-7
	// http://tools.ietf.org/html/rfc4422#section-5
	// http://tools.ietf.org/html/rfc2898#section-5.2
	private Void authScramSha1(Str databaseName, Str userName, Str password) {
		gs2Header	:= "n,,"	// no idea where this comes from!
		
		// ---- 1st message ----
		random			:= Random.makeSecure
		clientNonce		:= Buf().writeI8(random.next).writeI8(random.next).toBase64
		clientFirstMsg	:= "n=${userName},r=${clientNonce}"
		serverFirstRes	:= Operation(this).runCommand("${databaseName}.\$cmd", map
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
		hashedPassword	:= "${userName}:mongo:${password}".toBuf.toDigest("MD5").toHex
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
		serverSecondRes := Operation(this).runCommand("${databaseName}.\$cmd", map
			.add("saslContinue", 1)
			.add("conversationId", conversationId)
			.add("payload", Buf().print(clientFinal))
		)
		serverSecondMsg	:= ((Buf) serverSecondRes["payload"]).readAllStr
		payloadValues	= Str:Str[:].addList(serverSecondMsg.split(',')) { it[0..<1] }.map { it[2..-1] }
		serverProof		:= payloadValues["v"]

		// authenticate the server
		if (serverSignature != serverProof)
			throw MongoErr(ErrMsgs.connection_invalidServerSignature(serverSignature, serverProof))

		// ---- 3rd message ----
		serverThirdRes := Operation(this).runCommand("${databaseName}.\$cmd", map
			.add("saslContinue", 1)
			.add("conversationId", conversationId)
			.add("payload", Buf())
		)
		if (serverThirdRes["done"] != true)
			throw MongoErr(ErrMsgs.connection_scramNotDone(serverThirdRes.toStr))
	}
	
	private Buf xor(Buf key, Buf sig) {
		out := Buf()
		key.size.times {
			out.write(key.read.xor(sig.read))
		}
		return out.flip
	}
	
	private Void authMongoDbCr(Str databaseName, Str userName, Str password) {
		nonce 	:= (Str) Operation(this).runCommand("${databaseName}.\$cmd", ["getnonce": 1])["nonce"]
		passdig	:= "${userName}:mongo:${password}".toBuf.toDigest("MD5").toHex
		digest	:=  ( nonce + userName + passdig ).toBuf.toDigest("MD5").toHex
		authCmd	:= Str:Obj?[:] { ordered = true }
			.add("authenticate", 1)
			.add("user",	userName)
			.add("nonce",	nonce)
			.add("key",		digest)
		Operation(this).runCommand("${databaseName}.\$cmd", authCmd)		
	}
	
	override Void logout(Str databaseName, Bool checked := true) {
		try {
			Operation(this).runCommand("${databaseName}.\$cmd", ["logout": 1])
			authentications.remove(databaseName)
		} catch (Err err) {
			if (checked) throw err
		}
	}

	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		isClosed ? "Closed" : socket.remoteAddr.toStr
	}

	private [Str:Obj?] 	map() { Str:Obj?[:] { ordered = true } }
}

