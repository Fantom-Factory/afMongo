using concurrent
using afConcurrent
using inet

** Manages a pool of connections. 
** 
** Connections are created on-demand and kept in a pool when idle. 
** 
** Note this connection manager is safe for multi-threaded / web-application use.
//** Once the pool is exhausted, any operation requiring a connection will block (for 'maxWaitTime') 
//** waiting for an available connection.
const class ConnectionManagerPooled : ConnectionManager {
	private const SynchronizedState connectionState
	private const OneShotLock		shutdownLock
	
	** The maximum number of database connections this pool should open.
	** Set it to the number of concurrent users you expect to use your application.
	const Int 		maxNoOfConnections	:= 10

	** The maximum time a thread may wait for a connection to become available.
//	const Duration	maxWaitTime			:= 10sec
	
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
	
	@NoDoc	// nothing interesting to add here
	override Obj? leaseConnection(|Connection->Obj?| c) {
		connection := checkOut
		try {
			obj := c(connection)
			return obj
		} finally {
			checkIn(connection)
		}
	}

	@NoDoc	// nothing interesting to add here
	override Void shutdown() {
		shutdownLock.lock
		
		// TODO: wait for used sockets to be checked in
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

//		default wait time = 200ms -> is an eternity for computers, tiny for humans. set as a public NoDoc field 
		
		return (Connection) connectionState.getState |ConnectionManagerPoolState state->Unsafe?| {
			if (!state.checkedIn.isEmpty) {
				connection := state.checkedIn.pop
				state.checkedOut.push(connection)
				return Unsafe(connection)
			}
			
			if (state.checkedOut.size >= maxNoOfConnections)
				// TODO: return empty handed & wait for a free one
				throw MongoErr("Argh! No more connections! All ${maxNoOfConnections} are in use!")
			
			connection := state.connectionFactory()
			state.checkedOut.push(connection)
			return Unsafe(connection)
		}?->val
	}

	private Void checkIn(Connection connection) {
		unsafeConnection := Unsafe(connection)
		connectionState.withState |ConnectionManagerPoolState state| {
			conn := (Connection) unsafeConnection.val
			state.checkedOut.removeSame(conn)
			
			// make sure we don't save stale connections
			if (!conn.isClosed)
				state.checkedIn.push(conn)

		// call get() to make sure this thread checks in before it asks for a new one
		}.get	
	}
}

internal class ConnectionManagerPoolState {
	Connection[]	checkedOut	:= [,]
	Connection[]	checkedIn	:= [,]
	|->Connection|?	connectionFactory
}