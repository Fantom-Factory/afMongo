using inet::IpAddr
using inet::TcpSocket

** Represents a connection to a MongoDB instance.
@NoDoc	// advanced use only
mixin MongoConn {
	
	abstract Log		log()

	** Data *from* MongoDB *to* the client.
	abstract InStream 	in()
	
	** Data *to* MongoDB *from* the client.
	abstract OutStream	out()
	
	** Closes the connection.
	** Should never throw an IOErr.
	abstract Void		close()	
	
	** Return 'true' if this socket is closed. 
	abstract Bool		isClosed()
	
	** Has this connection been authenticated?
	abstract Bool		isAuthenticated()
	
	** The preferred negotiated compressor supported by both the server and the driver.
	** May be one of 'snappy, 'zlib', or 'zstd'.
	abstract Str?		compressor()
	
	** The compression level (0 - 9) to use with zlib.
	abstract Int?		zlibCompressionLevel()
	
	** Returns the Session associated with this connection.
	** Sessions are checked out lazily.
	internal
	abstract MongoSess	getSession()

	** Jailbreaks the attached MongoSession from this connection.
	** Returns 'null' if the session has already been detached, or was never created.
	internal
	abstract MongoSess?	detachSession()
}

** Connects to MongoDB via an 'inet::TcpSocket'.
internal class MongoTcpConn : MongoConn {
	override Log			log
			 TcpSocket		socket
			 Uri?			mongoUrl			// used by MongoConnMgrPool
			 Bool			forceCloseOnCheckIn	// used by MongoConnMgrPool
	override Bool			isAuthenticated
	override Str?			compressor
	override Int?			zlibCompressionLevel
	private	 MongoSessPool?	sessPool
	private	 MongoSess?		sess
	
	** Used by ConnPool
	** Allows you to pass in a TcpSocket with options already set.
	new fromSocket(TcpSocket socket, Log log, MongoSessPool sessPool) {
		this.socket 	= socket
		this.log		= log
		this.sessPool	= sessPool
	}

	** Used by MongoSafari
	** Creates a new TCP Socket
	new make(Bool ssl, Log log) {
		this.socket 	= newSocket(ssl)
		this.log		= log
	}
	
	This connect(Str address, Int port) {
		try {
			socket.connect(IpAddr(address), port)
			return this
		}
		catch (Err err)
			throw IOErr("Could not connect to MongoDB at `${address}:${port}` - ${err.msg}", err)
	}
	
	override MongoSess getSession() {
		if (sess != null)
			return sess
		
		if (sessPool == null)
			throw Err("Wot no SessPool???")

		return sess = sessPool.checkout
	}

	override MongoSess? detachSession() {
		sess := this.sess
		this.sess = null
		return sess
	}
	
	override InStream	in()		{ socket.in			}
	override OutStream	out()		{ socket.out		}
	override Void		close()		{ socket.close		}
	override Bool		isClosed()	{ socket.isClosed	}
	
	** Retain backwards compatibility with all recent versions of Fantom.
	private static TcpSocket newSocket(Bool ssl) {
		socket	 := null as TcpSocket
		oldSkool := Pod.find("inet").version < Version("1.0.77")
		if (oldSkool)
			socket = ssl ? TcpSocket#.method("makeTls").call : TcpSocket#.method("make").call
		else {
			socket = TcpSocket#.method("make").call
			if (ssl)
				socket = socket->upgradeTls
		}
		return socket
	}

	@NoDoc
	override Str toStr() {
		isClosed ? "Closed" : socket.remoteAddr.toStr
	}
}

