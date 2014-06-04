using concurrent
using afConcurrent
using inet

** Manages a pool of connections. 
** 
** Connections are created on-demand and kept in a pool when idle. 
** 
** Note this connection manager *is* safe for multi-threaded / web-application use.
//** Once the pool is exhausted, any operation requiring a connection will block (for 'maxWaitTime') 
//** waiting for an available connection.
const class ConnectionManagerPooled : ConnectionManager {
	private const OneShotLock		startupLock		:= OneShotLock("Connection Pool has been started")
	private const OneShotLock		shutdownLock	:= OneShotLock("Connection Pool has been shutdown")
	private const SynchronizedState connectionState
	
	** The URI used to connect to MongoDB.
	** 
	**   `mongodb://username:password@example1.com/puppies?maxPoolSize=50`
	const Uri		mongoUri
	
	** The minimum number of database connections this pool should keep open.
	** They are opened on 'startup()'.
	const Int 		minNoOfConnections	:= 0

	** The maximum number of database connections this pool should open.
	** Set it to the number of concurrent users you expect to use your application.
	const Int 		maxNoOfConnections	:= 10

	** The maximum time a thread may wait for a connection to become available.
//	const Duration	maxWaitTime			:= 10sec
	
//	** Ctor for advanced usage.
//	new make(ActorPool actorPool, |->Connection| connectionFactory, |This|? f := null) {
//		f?.call(this)	
//		this.connectionState 	= SynchronizedState(actorPool, ConnectionManagerPoolState#)
//		
//		// given it's only ever going to be used inside the state thread, it should be safe to unsafe it over
//		sFactory := Unsafe(connectionFactory).toImmutable
//		connectionState.withState |ConnectionManagerPoolState state| {
//			state.connectionFactory = sFactory.val
//		}.get
//	}

	** Create a 'ConnectionManager' from a [Mongo Connection URI]`http://docs.mongodb.org/manual/reference/connection-string/`.
	** If user credentials are supplied, they are used as default authentication for each connection.
	** 
	** The following Uri options are supported:
	**  - [minPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.minPoolSize`
	**  - [maxPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.maxPoolSize`
	** 
	** TODO: connectTimeoutMS
	** TODO: socketTimeoutMS
	** TODO: Write Concern Options
	** 
	** URI examples:
	**   `mongodb://username:password@example1.com/database?maxPoolSize=50`
	**   `mongodb://example2.com?minPoolSize=10&maxPoolSize=50`
	** 
	** @see `http://docs.mongodb.org/manual/reference/connection-string/`
	new makeFromUri(ActorPool actorPool, Uri mongoConnectionUri) {
		if (mongoConnectionUri.scheme != "mongodb")
			throw ArgErr(ErrMsgs.connectionManager_badScheme(mongoConnectionUri))
		
		this.mongoUri			= mongoConnectionUri
		this.connectionState 	= SynchronizedState(actorPool, ConnectionManagerPoolState#)
		this.minNoOfConnections = mongoUri.query["minPoolSize"]?.toInt ?: minNoOfConnections
		this.maxNoOfConnections = mongoUri.query["maxPoolSize"]?.toInt ?: maxNoOfConnections
		
		username := mongoUri.userInfo?.split(':')?.getSafe(0) ?: Str.defVal
		password := mongoUri.userInfo?.split(':')?.getSafe(1) ?: Str.defVal
		address	 := mongoUri.host ?: "127.0.0.1"
		port	 := mongoUri.port ?: 27017
		database := mongoUri.pathOnly.toStr as Str	// implicit cast to Str?
		
		if (database.startsWith("/"))
			database = database[1..-1]
		if (database.isEmpty)
			database = null
		
		if ((username.isEmpty).xor(password.isEmpty))
			throw ArgErr(ErrMsgs.connectionManager_badUsernamePasswordCombo(username, password, mongoUri))
		
		conFactory := |->Connection| {
			connection 	:= TcpConnection(IpAddr(address), port)
			
			// perform some default database authentication
			if (!username.isEmpty && password.isEmpty) {
				authDb	:= Database(this, database ?: "admin")
				authCmd := authDb.authCmd(username, password)
				Operation(connection).runCommand("${authDb.name}.\$cmd", authCmd)
			}
			
			return connection
		}
		
		// given it's only ever going to be used inside the state thread, it should be safe to unsafe it over
		sFactory := Unsafe(conFactory).toImmutable
		connectionState.withState |ConnectionManagerPoolState state| {
			state.connectionFactory = sFactory.val
		}.get
	}
	
	@NoDoc	// nothing interesting to add here
	override Obj? leaseConnection(|Connection->Obj?| c) {
		connection := checkOut
		try {
			obj := c(connection)
			return obj
		} finally {
			// FIXME: need to re-authenticate with default user
			checkIn(connection)
		}
	}
	
	@NoDoc	// nothing interesting to add here
	override This startup() {
		if (startupLock.locked)
			return this
		startupLock.lock

		if (minNoOfConnections < 0)
			throw ArgErr(ErrMsgs.connectionManager_badMinConnectionSize(minNoOfConnections, mongoUri))
		if (maxNoOfConnections < 1)
			throw ArgErr(ErrMsgs.connectionManager_badMaxConnectionSize(maxNoOfConnections, mongoUri))
		if (minNoOfConnections > maxNoOfConnections)
			throw ArgErr(ErrMsgs.connectionManager_badMinMaxConnectionSize(minNoOfConnections, maxNoOfConnections, mongoUri))
		
		// connect x times
		(1..minNoOfConnections).toList.map { checkOut }.each { checkIn(it) }
		
		return this
	}

	@NoDoc	// nothing interesting to add here
	override This shutdown() {
		shutdownLock.lock
		
		// TODO: wait for used sockets to be checked in
		connectionState.withState |ConnectionManagerPoolState state| {
			state.connectionFactory = null
			
			state.checkedIn.each { it.close }
			state.checkedIn.clear

			// TODO: Wait!
			state.checkedOut.each { it.close }
			state.checkedOut.clear
		}.get

		return this
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