using inet::IpAddr
using inet::TcpSocket
using concurrent::AtomicInt

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
	internal Uri?			_mongoUrl
	internal Bool			_forceCloseOnCheckIn
	internal Bool			_isAuthenticated
	internal Duration?		_lingeringSince
	internal MongoSessPool?	_sessPool
	internal MongoSess?		_sess
	internal Int			_id					:= _nextId.getAndIncrement
	private	 Int			_numCheckouts
	private  Duration		_dob				:= Duration.now
	private  Duration?		_checkOutTs
	private  Duration?		_lastUseTime
	
	static	const
	private	AtomicInt		_nextId				:= AtomicInt(1)

	** Creates a fresh, detached, socket using the same host, port, and tls settings.
	abstract
	internal MongoConn	_refresh(Uri mongoUrl)
	
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
	
	** Associates (or clears) a jailbroken session with this connection. 
	internal Void _setSession(MongoSess? session) {
		if (this._sess != null && session != null)
			throw Err("Cannot setSession(), I've already got one - $_sess")

		if (session?.isDetached == false)
			throw Err("Cannot setSession(), Session is NOT detached - $session")

		this._sess = session
	}
	
	** Allow new connections to *linger* / stay alive (by 'maxIdleTimeMS') before being closed to avoid socket throttling during bursts of activity. 
	internal Bool _isStale(Duration maxLinger) {
		if (_lingeringSince == null) return false
		ttl := _lingeringSince + maxLinger - Duration.now
		return ttl < 1ms
	}
	
	internal Str:Str _props() {
		lingerTime	:= _lingeringSince == null ? null : (Duration.now - _lingeringSince).floor(1ms)
		aliveTime	:= (Duration.now - _dob).floor(1ms)
		id			:= _id.toHex(4).upper

		return Str:Obj?[:].with |m| {
			m.ordered 						= true
			m.add("conn.${id}.inUse"		, _checkOutTs != null)
			m.add("conn.${id}.numUses"		, _numCheckouts)
			m.add("conn.${id}.lingeringFor"	, lingerTime)
			m.add("conn.${id}.aliveFor"		, aliveTime)
			m.add("conn.${id}.lastUseTime"	, _lastUseTime)
		}
	}

	internal Void _onCheckOut() {
		_numCheckouts++
		_checkOutTs = Duration.now
	}
	
	internal Void _onCheckIn() {
		_lastUseTime = (Duration.now - _checkOutTs).floor(1ms)
		_checkOutTs = null
	}
}

** Connects to MongoDB via an 'inet::TcpSocket'.
internal class MongoTcpConn : MongoConn {
	override Log			log
	private	 TcpSocket		socket
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
	
	override MongoConn _refresh(Uri mongoUrl) {
		socket.close

		// when retrying a cmd, avoid errors like: "Command 'saslStart' failed. MongoDB says: no SNI name sent, make sure using a MongoDB 3.4+ driver/shell."
		// this happens when we connect using a IP address and not a host name
		// (but only on Atlas sharded clusters)
		// so keep the original connection addr (host) and use it to refresh and reconnect!

		// Good discussion on TLS SNI support in Java: (TLDR - it's fixed in Java 7)
		// https://issues.apache.org/jira/browse/HTTPCLIENT-1119
		// https://github.com/twisted/txmongo/issues/236

		// this class contains too much meta (sessions, compression, etc) to just ditch,
		// so let's just replace the internal socket obj
		// todo - this new socket WILL NOT have the correct timeouts as specified by MongoConnUrl
		// so we'll just close it when finished - all new sockets in the pool should be fine.
		this._forceCloseOnCheckIn	= true
		this._isAuthenticated		= false
		this._mongoUrl				= mongoUrl
		this.socket					= newSocket(ssl)
		connect(mongoUrl.host, mongoUrl.port)
		return this
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
