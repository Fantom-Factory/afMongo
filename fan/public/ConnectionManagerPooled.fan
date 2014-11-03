using concurrent
using afConcurrent
using inet

** Manages a pool of connections. 
** 
** Connections are created on-demand and kept in a pool when idle. 
** Once the pool is exhausted, any operation requiring a connection will block for (at most) 'waitQueueTimeout' 
** waiting for an available connection.
** 
** Note this connection manager *is* safe for multi-threaded / web-application use.
const class ConnectionManagerPooled : ConnectionManager {
	private const Log				log				:= Utils.getLog(ConnectionManagerPooled#)
	private const OneShotLock		startupLock		:= OneShotLock("Connection Pool has been started")
	private const OneShotLock		shutdownLock	:= OneShotLock("Connection Pool has been shutdown")
	private const SynchronizedState connectionState
	
	** The host name of the MongoDB server this 'ConnectionManager' connects to.
	override const Uri mongoUrl

	** The default write concern that all write operations use if none supplied.
	** 
	** Defaults to '["w": 1, "wtimeout": 0, "journal": false]'
	**  - write operations are acknowledged,
	**  - write operations never time out,
	**  - write operations need not be committed to the journal.
	override const Str:Obj? writeConcern := ["w": 1, "wtimeout": 0, "journal": false]
	
	** The default database connections are authenticated against.
	** 
	** Set via the `#connectionUrl`.
	const Str?	defaultDatabase
	
	** The default username connections are authenticated with.
	** 
	** Set via the `#connectionUrl`.
	const Str?	defaultUsername
	
	** The default password connections are authenticated with.
	** 
	** Set via the `#connectionUrl`.
	const Str?	defaultPassword
	
	** The original URL this 'ConnectionManager' was configured with.
	** May contain authentication details.
	** 
	**   mongodb://username:password@example1.com/puppies?maxPoolSize=50
	const Uri	connectionUrl
	
	** The minimum number of database connections this pool should keep open.
	** They are initially created during 'startup()'.
	** 
	** Set via the [minPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.minPoolSize` connection string option.
	** Defaults to 0.
	** 
	**   mongodb://example.com/puppies?minPoolSize=50
	const Int 	minPoolSize	:= 0

	** The maximum number of database connections this pool should open.
	** This is the number of concurrent users you expect to use your application.
	** 
	** Set via the [maxPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.maxPoolSize` connection string option.
	** Defaults to 10.
	** 
	**   mongodb://example.com/puppies?maxPoolSize=10
	const Int 	maxPoolSize	:= 10
	
	** The maximum time a thread can wait for a connection to become available.
	** 
	** Set via the [maxPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.waitQueueTimeoutMS` connection string option.
	** Defaults to 10 seconds.
	** 
	**   mongodb://example.com/puppies?waitQueueTimeoutMS=10
	const Duration	waitQueueTimeout := 10sec

	** If specified, this is the time to attempt a connection before timing out.
	** If 'null' (the default) then a system timeout is used.
	** 
	** Set via the [connectTimeoutMS]`http://docs.mongodb.org/manual/reference/connection-string/#uri.connectTimeoutMS` connection string option.
	** 
	**   mongodb://example.com/puppies?connectTimeoutMS=2500
	** 
	** Equates to `inet::SocketOptions.connectTimeout`.
	const Duration? connectTimeout
	
	** If specified, this is the time to attempt a send or receive on a socket before the attempt times out.
	** 'null' (the default) indicates an infinite timeout.
	** 
	** Set via the [socketTimeoutMS]`http://docs.mongodb.org/manual/reference/connection-string/#uri.socketTimeoutMS` connection string option.
	** 
	**   mongodb://example.com/puppies?socketTimeoutMS=2500
	** 
	** Equates to `inet::SocketOptions.receiveTimeout`.
	const Duration? socketTimeout

	** When the connection pool is shutting down, this is the amount of time to wait for all connections for close before they are forcibly closed.
	** 
	** Defaults to '2sec'. 
	const Duration? shutdownTimeout	:= 2sec
	
	// used to test the backoff func
	internal const |Range->Int|	randomFunc	:= |Range r->Int| { r.random }
	internal const |Duration| 	sleepFunc	:= |Duration napTime| { Actor.sleep(napTime) }
	
	** Create a 'ConnectionManager' from a [Mongo Connection URL]`http://docs.mongodb.org/manual/reference/connection-string/`.
	** If user credentials are supplied, they are used as default authentication for each connection.
	** 
	** The following Url options are supported:
	**  - [minPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.minPoolSize`
	**  - [maxPoolSize]`http://docs.mongodb.org/manual/reference/connection-string/#uri.maxPoolSize`
	**  - [waitQueueTimeoutMS]`http://docs.mongodb.org/manual/reference/connection-string/#uri.waitQueueTimeoutMS`
	**  - [connectTimeoutMS]`http://docs.mongodb.org/manual/reference/connection-string/#uri.connectTimeoutMS`
	**  - [socketTimeoutMS]`http://docs.mongodb.org/manual/reference/connection-string/#uri.socketTimeoutMS`
	**  - [w]`http://docs.mongodb.org/manual/reference/connection-string/#uri.w`
	**  - [wtimeoutMS]`http://docs.mongodb.org/manual/reference/connection-string/#uri.wtimeoutMS`
	**  - [journal]`http://docs.mongodb.org/manual/reference/connection-string/#uri.journal`
	** 
	** URL examples:
	**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
	**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50'
	** 
	** @see `http://docs.mongodb.org/manual/reference/connection-string/`
	new makeFromUrl(ActorPool actorPool, Uri connectionUrl, |This|? f := null) {
		if (connectionUrl.scheme != "mongodb")
			throw ArgErr(ErrMsgs.connectionManager_badScheme(connectionUrl))

		this.mongoUrl			= connectionUrl
		this.connectionUrl		= connectionUrl
		this.connectionState	= SynchronizedState(actorPool, ConnectionManagerPoolState#)
		this.minPoolSize 		= mongoUrl.query["minPoolSize"]?.toInt ?: minPoolSize
		this.maxPoolSize 		= mongoUrl.query["maxPoolSize"]?.toInt ?: maxPoolSize
		waitQueueTimeoutMs		:= mongoUrl.query["waitQueueTimeoutMS"]?.toInt
		connectTimeoutMs		:= mongoUrl.query["connectTimeoutMS"]?.toInt
		socketTimeoutMs 		:= mongoUrl.query["socketTimeoutMS"]?.toInt
		w						:= mongoUrl.query["w"]
		wtimeoutMs		 		:= mongoUrl.query["wtimeoutMS"]?.toInt
		journal			 		:= mongoUrl.query["journal"]?.toBool

		if (minPoolSize < 0)
			throw ArgErr(ErrMsgs.connectionManager_badInt("minPoolSize", "zero", minPoolSize, mongoUrl))
		if (maxPoolSize < 1)
			throw ArgErr(ErrMsgs.connectionManager_badInt("maxPoolSize", "one", maxPoolSize, mongoUrl))
		if (minPoolSize > maxPoolSize)
			throw ArgErr(ErrMsgs.connectionManager_badMinMaxConnectionSize(minPoolSize, maxPoolSize, mongoUrl))		
		if (waitQueueTimeoutMs != null && waitQueueTimeoutMs < 0)
			throw ArgErr(ErrMsgs.connectionManager_badInt("waitQueueTimeoutMS", "zero", waitQueueTimeoutMs, mongoUrl))
		if (connectTimeoutMs != null && connectTimeoutMs < 0)
			throw ArgErr(ErrMsgs.connectionManager_badInt("connectTimeoutMS", "zero", connectTimeoutMs, mongoUrl))
		if (socketTimeoutMs != null && socketTimeoutMs < 0)
			throw ArgErr(ErrMsgs.connectionManager_badInt("socketTimeoutMS", "zero", socketTimeoutMs, mongoUrl))
		if (wtimeoutMs != null && wtimeoutMs < 0)
			throw ArgErr(ErrMsgs.connectionManager_badInt("wtimeoutMS", "zero", wtimeoutMs, mongoUrl))

		if (waitQueueTimeoutMs != null)
			waitQueueTimeout = (waitQueueTimeoutMs * 1000000).toDuration
		if (connectTimeoutMs != null)
			connectTimeout = (connectTimeoutMs * 1000000).toDuration
		if (socketTimeoutMs != null)
			socketTimeout = (socketTimeoutMs * 1000000).toDuration

		address	 := mongoUrl.host ?: "127.0.0.1"
		port	 := mongoUrl.port ?: 27017
		database := trimToNull(mongoUrl.pathOnly.toStr)
		username := trimToNull(mongoUrl.userInfo?.split(':')?.getSafe(0))
		password := trimToNull(mongoUrl.userInfo?.split(':')?.getSafe(1))
		
		if ((username == null).xor(password == null))
			throw ArgErr(ErrMsgs.connectionManager_badUsernamePasswordCombo(username, password, mongoUrl))

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
				socket := TcpSocket()
				socket.options.connectTimeout = connectTimeout
				socket.options.receiveTimeout = socketTimeout
				return TcpConnection(socket).connect(IpAddr(address), port)
			} 
		}.get
		
		writeConcern := Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("journal", false)
		if (w != null)
			writeConcern["w"] = Int.fromStr(w, 10, false) != null ? w.toInt : w
		if (wtimeoutMs != null)
			writeConcern["wtimeout"] = wtimeoutMs
		if (journal != null)
			writeConcern["journal"] = journal
		this.writeConcern = writeConcern

		query := mongoUrl.query.rw
		query.remove("minPoolSize")
		query.remove("maxPoolSize")
		query.remove("waitQueueTimeoutMS")
		query.remove("connectTimeoutMS")
		query.remove("socketTimeoutMS")
		query.remove("w")
		query.remove("wtimeoutMS")
		query.remove("journal")
		query.each |val, key| {
			log.warn(LogMsgs.connectionManager_unknownUrlOption(key, val, mongoUrl))
		}
		
		// remove user credentials and other crud from the url
		this.mongoUrl = "mongodb://${address}:${port}".toUri	// F4 doesn't like Uri interpolation
		
		// allow the it-block to override the default settings
		// no validation occurs - only used for testing.
		f?.call(this)
	}
	
	** Creates the initial pool and establishes 'minPoolSize' connections with the server.
	override ConnectionManager startup() {
		if (startupLock.locked)
			return this
		startupLock.lock

		// connect x times
		minPoolSize.times { checkIn(checkOut) }
		
		return this
	}
	
	** Makes a connection available to the given function.
	** 
	** If all connections are currently in use, a truncated binary exponential backoff algorithm 
	** is used to wait for one to become free. If, while waiting, the duration specified in 
	** 'waitQueueTimeout' expires then a 'MongoErr' is thrown.
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
	
	** Closes all connections. 
	** Initially waits for 'shutdownTimeout' for connections to finish what they're doing before 
	** they're closed. After that, all open connections are forcibly closed regardless of whether 
	** they're in use or not.
	override ConnectionManager shutdown() {
		shutdownLock.lock
		
		closeFunc := |->Bool?| {
			waitingOn := connectionState.getState |ConnectionManagerPoolState state -> Int| {
				while (!state.checkedIn.isEmpty) {
					state.checkedIn.removeAt(0).close 
				}
				return state.checkedOut.size
			}
			if (waitingOn > 0)
				log.info(LogMsgs.connectionManager_waitingForConnectionsToClose(waitingOn))
			return waitingOn > 0 ? null : true
		}
		
		allClosed := backoffFunc(closeFunc, shutdownTimeout) ?: false

		if (!allClosed) {
			// too late, they've had their chance. Now everybody dies.
			connectionState.withState |ConnectionManagerPoolState state| {
				// just in case one or two snuck back in
				while (!state.checkedIn.isEmpty) {
					state.checkedIn.removeAt(0).close 
				}
				
				// DIE! DIE! DIE!
				while (!state.checkedOut.isEmpty) {
					state.checkedOut.removeAt(0).close 
				}
			}.get
		}
		
		return this
	}
	
	** Implements a truncated binary exponential backoff algorithm. *Damn, I'm good!*
	** Returns 'null' if the operation timed out.
	** 
	** @see `http://en.wikipedia.org/wiki/Exponential_backoff`
	internal Obj? backoffFunc(|Duration totalNapTime->Obj?| func, Duration timeout) {
		result			:= null
		c				:= 0
		i				:= 10
		totalNapTime	:= 0ms
		
		while (result == null && totalNapTime < timeout) {

			result = func.call(totalNapTime)

			if (result == null) {
				if (++c > i) c = i	// truncate the exponentiation ~ 10 secs
				napTime := (randomFunc(0..<2.pow(c)) * 10 * 1000000).toDuration

				// don't over sleep!
				if ((totalNapTime + napTime) > timeout)
					napTime = timeout - totalNapTime 

				sleepFunc(napTime)
				totalNapTime += napTime
				
				// if we're about to quit, lets have 1 more last ditch attempt!
				if (totalNapTime >= timeout)
					result = func.call(totalNapTime)
			}
		}
		
		return result
	}
	
	private Connection checkOut() {
		shutdownLock.check
		
		connectionFunc := |Duration totalNapTime->Connection?| {
			con := connectionState.getState |ConnectionManagerPoolState state->Unsafe?| {
				if (!state.checkedIn.isEmpty) {
					connection := state.checkedIn.pop
					state.checkedOut.push(connection)
					return Unsafe(connection)
				}
				
				if (state.checkedOut.size < maxPoolSize) {
					connection := state.connectionFactory()
					state.checkedOut.push(connection)
					return Unsafe(connection)
				}
				
				return null
			}?->val
			
			// let's not swamp the logs the first time we can't connect
			// 1.5 secs gives at least 6 connection attempts
			if (con == null && totalNapTime > 1.5sec)
				log.warn(LogMsgs.connectionManager_waitingForConnectionsToFree(maxPoolSize))

			return con
		}

		connection
			:= (Connection?) backoffFunc(connectionFunc, waitQueueTimeout)
			?: throw MongoErr("Argh! No more connections! All ${maxPoolSize} are in use!")
		
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