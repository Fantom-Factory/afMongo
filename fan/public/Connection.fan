using inet

@NoDoc
mixin Connection {
	abstract InStream 	in()
	abstract OutStream	out()
	abstract Void		close()	
}

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

	override InStream	in()	{ socket.in		}
	override OutStream	out()	{ socket.out	}
	override Void		close()	{ socket.close	}
}

