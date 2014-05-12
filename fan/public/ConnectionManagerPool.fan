using concurrent
using afConcurrent
using inet

const class ConnectionManagerPool : ConnectionManager {
	private const SynchronizedState connectionState
	private const OneShotLock		shutdownLock
	
	** The maximum number of database connections this pool should open.
	** Set it to the number of concurrent users you expect to use your application.
	const Int maxNoOfConnections	:= 5
	
	new make(ActorPool actorPool, |->Connection| connectionFactory, |This|? f := null) {
		f?.call(this)	
		this.connectionState 	= SynchronizedState(actorPool, ConnectionManagerPoolState#)
		this.shutdownLock		= OneShotLock("Connection Pool has been shutdown")
		
		// given it's only ever going to be used inside the state thread, it should be safe to unsafe it over
		sFactory := Unsafe(connectionFactory).toImmutable
		connectionState.withState |ConnectionManagerPoolState state| {
			state.connectionFactory = sFactory.val
		}.get
	}
	
	new makeWithIpAddr(ActorPool actorPool, IpAddr ipAddr := IpAddr("127.0.0.1"), Int port := 27017, SocketOptions? options := null, |This|? f := null) : this.make(actorPool, |->Connection| { TcpConnection(ipAddr, port, options) }) { }
	
	override Obj? leaseConnection(|Connection->Obj?| c) {
		connection := checkOut
		try {
			obj := c(connection)
			return obj
		} finally {
			checkIn(connection)
		}
	}

	override Void shutdown() {
		shutdownLock.lock
		
		// TODO: wait for close sockets to be checked in
		connectionState.withState |ConnectionManagerPoolState state| {
			state.connectionFactory = null
			
			state.checkedIn.each { it.close }
			state.checkedIn.clear

			// TODO: Wait!
			state.checkedOut.each { it.close }
			state.checkedOut.clear
		}
	}
	
	private Connection checkOut() {
		shutdownLock.check
		// TODO: log warning if all in use, and set timeout for max wait and re-tries

		return (Connection) connectionState.getState |ConnectionManagerPoolState state->Unsafe?| {
			if (!state.checkedIn.isEmpty) {
				connection := state.checkedIn.pop
				state.checkedOut.push(connection)
				return Unsafe(connection)
			}
			// TODO: check max size
			connection := state.connectionFactory()
			state.checkedOut.push(connection)
			return Unsafe(connection)
		}?->val
	}

	private Void checkIn(Connection connection) {
		unsafeConnection := Unsafe(connection)
		connectionState.withState |ConnectionManagerPoolState state| {
			state.checkedOut.removeSame(unsafeConnection.val)
			state.checkedIn.push(unsafeConnection.val)
		}.get	// call get to make sure this thread checks in before it asks for a new one
	}
}

internal class ConnectionManagerPoolState {
	Connection[]	checkedOut	:= [,]
	Connection[]	checkedIn	:= [,]
	|->Connection|?	connectionFactory
}