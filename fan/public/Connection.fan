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
	
	abstract Bool		isAuthenticated()
}

** Connects to MongoDB via an 'inet::TcpSocket'.
@NoDoc
class TcpConnection : Connection {
			 TcpSocket	socket
			 Uri?		mongoUrl			// used by MongoConnMgrPool
			 Bool		forceCloseOnCheckIn	// used by MongoConnMgrPool
	override Bool		isAuthenticated
	
	** Allows you to pass in a TcpSocket with options already set.
	new fromSocket(TcpSocket socket) {
		this.socket = socket
	}

	** Creates a new TCP Socket
	new make(Bool ssl) {
		this.socket = newSocket(ssl)
	}
	
	This connect(Str address, Int port) {
		try {
			socket.connect(IpAddr(address), port)
			return this
		}
		catch (Err err)
			throw IOErr("Could not connect to MongoDB at `${address}:${port}` - ${err.msg}", err)
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

