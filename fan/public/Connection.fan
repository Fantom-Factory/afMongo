using inet

** Represents a connection to a MongoDB instance.
** All connections on creation should be connected to a MongoDB instance and ready to go.
** 
** @see `TcpConnection` 
mixin Connection {

	** Data *from* MongoDB *to* the client.
	abstract InStream 	in()
	
	** Data *to* MongoDB *from* the client.
	abstract OutStream	out()
	
	** Closes the connection.
	abstract Void		close()	
	
	** Return 'true' if this socket is closed. 
	abstract Bool		isClosed()
	
	** A map of databases -> users this connection is authenticated as. 
	abstract Str:Str	authentications
	
	** Authenticates this connection against a database with the given user credentials. 
	virtual Void authenticate(Str databaseName, Str userName, Str password) {
		nonce 	:= (Str) Operation(this).runCommand("${databaseName}.\$cmd", ["getnonce": 1])["nonce"]
		passdig	:= "${userName}:mongo:${password}".toBuf.toDigest("MD5").toHex
		digest	:=  ( nonce + userName + passdig ).toBuf.toDigest("MD5").toHex
		authCmd	:= Str:Obj?[:] { ordered = true }
			.add("authenticate", 1)
			.add("user",	userName)
			.add("nonce",	nonce)
			.add("key",		digest)
		Operation(this).runCommand("${databaseName}.\$cmd", authCmd)
		authentications[databaseName] = userName
	}
	
	** Logs this connection out from the given database.
	virtual Void logout(Str databaseName, Bool checked := true) {
		try {
			Operation(this).runCommand("${databaseName}.\$cmd", ["logout": 1])
			authentications.remove(databaseName)
		} catch (Err err) {
			if (checked) throw err
		}
	}	
}

** Connects to MongoDB via an 'inet::TcpSocket'.
@NoDoc
class TcpConnection : Connection {
	TcpSocket socket
	override Str:Str	authentications	:= [:]
	
	** Allows you to pass in a TcpSocket with options already set.
	new make(TcpSocket? socket := null) {
		this.socket = socket ?: TcpSocket()
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
	
	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		isClosed ? "Closed" : socket.remoteAddr.toStr
	}
}

