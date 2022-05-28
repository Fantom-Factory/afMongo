using inet::IpAddr
using inet::TcpSocket

** Represents a connection to a MongoDB instance.
@NoDoc	// advanced use only
abstract class MongoConn {
	
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
	
	// ----
	
	** The preferred negotiated compressor supported by both the server and the driver.
	** May be one of 'snappy, 'zlib', or 'zstd'.
	internal Str?			_compressor
	internal Int?			_zlibCompressionLevel
	// FIXME not used! / set
	internal Uri?			_mongoUrl
	internal Bool			_forceCloseOnCheckIn
	internal Bool			_isAuthenticated
	internal Duration?		_lingeringSince
	internal MongoSessPool?	_sessPool
	internal MongoSess?		_sess
	
	** Creates a fresh, detached, socket using the same host, port, and tls settings.
	abstract
	internal MongoConn	_refresh()
	
	** Returns the Session associated with this connection.
	** Sessions are checked out lazily.
	internal virtual MongoSess? _getSession(Bool createNew) {
		if (_sess != null)
			return _sess
		
		if (MongoTxn.cur != null)
			return MongoTxn.cur.sess
		
		if (createNew == false)
			return null

		if (_sessPool == null)
			throw Err("Wot no SessPool???")

		return _sess = _sessPool.checkout
	}

	** Jailbreaks the attached MongoSession from this connection.
	** Returns 'null' if the session has already been detached, or was never created.
	internal MongoSess? _detachSession() {
		sess := this._sess
		this._sess = null
		if (sess != null)
			sess.isDetached = true
		return sess
	}
	
	** Associates a jailbroken session with this connection. 
	internal Void _setSession(MongoSess? session) {
		if (session == null) return

		if (this._sess != null)
			throw Err("Cannot setSession(), I've already got one - $_sess")

		if (session.isDetached == false)
			throw Err("Cannot setSession(), Session is NOT detached - $_sess")

		this._sess = session
	}
	
	internal Bool _isStale(Duration maxLinger) {
		ttl := _lingeringSince + maxLinger - Duration.now
		return ttl < 1ms
	}
}

** Connects to MongoDB via an 'inet::TcpSocket'.
internal class MongoTcpConn : MongoConn {
	override Log			log
			 TcpSocket		socket
	private	 Bool			ssl

	** Used by ConnPool
	** Allows you to pass in a TcpSocket with options already set.
	new fromSocket(TcpSocket socket, Bool ssl, Log log, MongoSessPool sessPool) {
		this.socket 	= socket
		this.ssl		= ssl
		this.log		= log
		this._sessPool	= sessPool
	}

	** Used by MongoSafari
	** Creates a new TCP Socket
	new make(Bool ssl, Log log) {
		this.ssl		= ssl
		this.log		= log
		this.socket 	= newSocket(ssl)
	}

	This connect(Str address, Int port) {
		try {
			socket.connect(IpAddr(address), port)
			return this
		}
		catch (Err err)
			throw IOErr("Could not connect to MongoDB at ${address}:${port} - ${err.msg}", err)
	}

	override InStream	in()		{ socket.in			}
	override OutStream	out()		{ socket.out		}
	override Void		close()		{ socket.close		}
	override Bool		isClosed()	{ socket.isClosed	}
	
	override MongoConn _refresh() {
		MongoTcpConn(ssl, log).connect(socket.remoteAddr.numeric, socket.remotePort)
	}
	
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
