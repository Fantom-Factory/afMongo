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
	
	** The host name of the MongoDB server this 'ConnectionManager' connects to.
	override const Uri mongoUrl
	override Uri mongoUri() { mongoUrl }
	
	** The URI this 'ConnectionManager' was configured with.
	** 
	**   `mongodb://username:password@example1.com/puppies?maxPoolSize=50`
	const Uri	connectionUrl
	
	** The minimum number of database connections this pool should keep open.
	** They are initially created during 'startup()'.
	const Int 	minPoolSize	:= 0

	** The maximum number of database connections this pool should open.
	** Set it to the number of concurrent users you expect to use your application.
	const Int 	maxPoolSize	:= 10

	** The default database connections are authenticated against.
	const Str?	defaultDatabase
	
	** The default username connections are authenticated with.
	const Str?	defaultUsername
	
	** The default password connections are authenticated with.
	const Str?	defaultPassword
	
	** The maximum time a thread may wait for a connection to become available.
//	const Duration	maxWaitTime			:= 10sec

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
	new makeFromUri(ActorPool actorPool, Uri connectionUrl) {
		if (connectionUrl.scheme != "mongodb")
			throw ArgErr(ErrMsgs.connectionManager_badScheme(connectionUrl))
		
		this.mongoUrl			= connectionUrl
		this.connectionUrl		= connectionUrl
		this.connectionState	= SynchronizedState(actorPool, ConnectionManagerPoolState#)
		this.minPoolSize 		= mongoUri.query["minPoolSize"]?.toInt ?: minPoolSize
		this.maxPoolSize 		= mongoUri.query["maxPoolSize"]?.toInt ?: maxPoolSize
		
		if (minPoolSize < 0)
			throw ArgErr(ErrMsgs.connectionManager_badMinConnectionSize(minPoolSize, mongoUri))
		if (maxPoolSize < 1)
			throw ArgErr(ErrMsgs.connectionManager_badMaxConnectionSize(maxPoolSize, mongoUri))
		if (minPoolSize > maxPoolSize)
			throw ArgErr(ErrMsgs.connectionManager_badMinMaxConnectionSize(minPoolSize, maxPoolSize, mongoUri))		

		address	 := mongoUri.host ?: "127.0.0.1"
		port	 := mongoUri.port ?: 27017
		database := trimToNull(mongoUri.pathOnly.toStr)
		username := trimToNull(mongoUri.userInfo?.split(':')?.getSafe(0))
		password := trimToNull(mongoUri.userInfo?.split(':')?.getSafe(1))
		
		if ((username == null).xor(password == null))
			throw ArgErr(ErrMsgs.connectionManager_badUsernamePasswordCombo(username, password, mongoUri))

		if (database != null && database.startsWith("/"))
			database = trimToNull(database[1..-1])
		if (username != null && password != null && database == null)
			database = "admin"
		if (username == null && password == null)	// a default database has no meaning without credentials
			database = null
		
		defaultDatabase = database
		defaultUsername = username
		defaultPassword = password
		connectionState.withState |ConnectionManagerPoolState state| {
			state.connectionFactory = |->Connection| {
				TcpConnection(IpAddr(address), port)
			}
		}.get
		
		// remove user credentials and other crud from the uri
		mongoUrl = `mongodb://${address}:${port}`
	}
	
	** Makes a connection available to the given function.
	** 
	** All leased connections are authenticated against the default credentials.
	override Obj? leaseConnection(|Connection->Obj?| c) {
		connection := checkOut
		try {
			return c(connection)

		} catch (Err err) {
			// if something dies, kill the connection.
			// we may have died part way through talking with the server meaning our communication 
			// protocols are out of sync - rendering any future use of the connection useless.
			connection.close
			throw err
			
		} finally {
			checkIn(connection)
		}
	}
	
	** Creates the initial pool and establishes 'minPoolSize' connections with the server.
	override ConnectionManager startup() {
		if (startupLock.locked)
			return this
		startupLock.lock

		// connect x times
		(1..minPoolSize).toList.map { checkOut }.each { checkIn(it) }
		
		return this
	}

	** Closes all connections.
	override ConnectionManager shutdown() {
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
		
		connection := (Connection) connectionState.getState |ConnectionManagerPoolState state->Unsafe?| {
			if (!state.checkedIn.isEmpty) {
				connection := state.checkedIn.pop
				state.checkedOut.push(connection)
				return Unsafe(connection)
			}
			
			if (state.checkedOut.size >= maxPoolSize)
				// TODO: return empty handed & wait for a free one
				throw MongoErr("Argh! No more connections! All ${maxPoolSize} are in use!")
			
			connection := state.connectionFactory()
			state.checkedOut.push(connection)
			return Unsafe(connection)
		}?->val
		
		// ensure all connections are initially leased authenticated as the default user
		// specifically do the check here so you can always *brute force* an authentication on a connection
		if (defaultDatabase != null && connection.authentications[defaultDatabase] != defaultUsername)
			connection.authenticate(defaultDatabase, defaultUsername, defaultPassword)
		
		return connection
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
	
	private Str? trimToNull(Str? str) {
		(str?.trim?.isEmpty ?: true) ? null : str.trim
	}
}

internal class ConnectionManagerPoolState {
	Connection[]	checkedOut	:= [,]
	Connection[]	checkedIn	:= [,]
	|->Connection|?	connectionFactory
}