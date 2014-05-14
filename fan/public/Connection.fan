using inet

** Represents a connection to a MongoDB instance.
** All connections on creation should be connected to a MongoDB instance and ready to go.
** 
** @see `TcpConnection` 
// TODO: maybe connections should know if they're connected to a master, and who they're authenticated as? 
mixin Connection {
	
	** Data *from* MongoDB *to* the client.
	abstract InStream 	in()
	
	** Data *to* MongoDB *from* the client.
	abstract OutStream	out()
	
	** Closes the connection.
	abstract Void		close()	
	
	** Return 'true' if this socket is closed. 
	abstract Bool		isClosed()
	
	** Creates a TCP Connection to the given IP address.
	static Connection makeTcpConnection(IpAddr address := IpAddr("127.0.0.1"), Int port := 27017, SocketOptions? options := null) {
		TcpConnection(address, port, options)
	}
}

** Connects to MongoDB via an 'inet::TcpSocket'.
@NoDoc
class TcpConnection : Connection {
	TcpSocket socket
	
	new makeWithIpAddr(IpAddr address := IpAddr("127.0.0.1"), Int port := 27017, SocketOptions? options := null) {
		this.socket = TcpSocket()
		if (options != null)
			this.socket.options.copyFrom(options)
		socket.connect(address, port)
	}
	
	new makeWithSocket(TcpSocket socket) {
		this.socket = socket
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

