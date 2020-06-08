using concurrent::AtomicBool
using concurrent::AtomicRef
using concurrent::Actor
using concurrent::ActorPool
using afConcurrent::Synchronized
using afConcurrent::SynchronizedState
using inet::IpAddr
using inet::TcpSocket

** Manages a pool of connections. 
** 
** Connections are created on-demand and a total of 'minPoolSize' are kept in a pool when idle. 
** Once the pool is exhausted, any operation requiring a connection will block for (at most) 'waitQueueTimeout' 
** waiting for an available connection.
** 
** This connection manager is created with the standard [Mongo Connection URL]`https://docs.mongodb.org/manual/reference/connection-string/` in the format:
** 
**   mongodb://[username:password@]host[:port][/[defaultauthdb][?options]]
** 
** Examples:
** 
**   mongodb://localhost:27017
**   mongodb://username:password@example1.com/puppies?maxPoolSize=50
** 
** If connecting to a replica set then multiple hosts (with optional ports) may be specified:
** 
**   mongodb://db1.example.net,db2.example.net:2500/?connectTimeoutMS=30000
** 
** On 'startup()' the hosts are queried to find the primary / master node. 
** All read and write operations are performed on the primary node.
** 
** When a connection to the master node is lost, all hosts are re-queried to find the new master.
** 
** Note this connection manager *is* safe for multi-threaded / web-application use.
const class ConnectionManagerPooled : ConnectionManager {
	private const Log				log				:= ConnectionManagerPooled#.pod.log
	private const OneShotLock		startupLock		:= OneShotLock("Connection Pool has been started")
	private const OneShotLock		shutdownLock	:= OneShotLock("Connection Pool has been shutdown")
	private const AtomicBool 		failingOverRef	:= AtomicBool(false)
	private const Synchronized		failOverThread
	private const SynchronizedState connectionState

	** The host name of the MongoDB server this 'ConnectionManager' connects to.
	** When connecting to replica sets, this will indicate the primary.
	** 
	** This value is unavailable (returns 'null') until 'startup()' is called. 
	override Uri? mongoUrl() { mongoUrlRef.val }
	private const AtomicRef mongoUrlRef := AtomicRef(null)

	** The default write concern for all write operations. 
	** Set by specifying the 'w', 'wtimeoutMS' and 'journal' connection string options. 
	** 
	** Defaults to '["w": 1, "wtimeout": 0, "j": false]'
	**  - write operations are acknowledged,
	**  - write operations never time out,
	**  - write operations need not be committed to the journal.
	override const Str:Obj? writeConcern := ["w": 1, "wtimeout": 0, "j": false]
	
	** The default database connections are authenticated against.
	** 
	** Set via the `connectionUrl`.
	const Str?	defaultDatabase
	
	** The default username connections are authenticated with.
	** 
	** Set via the `connectionUrl`.
	const Str?	defaultUsername
	
	** The default password connections are authenticated with.
	** 
	** Set via the `connectionUrl`.
	const Str?	defaultPassword
	
	** The original URL this 'ConnectionManager' was configured with.
	** May contain authentication details.
	** 
	**   mongodb://username:password@example1.com/puppies?maxPoolSize=50
	const Uri	connectionUrl
	
	** The minimum number of database connections this pool should keep open.
	** They are initially created during 'startup()'.
	** 
	** Set via the [minPoolSize]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.minPoolSize` connection string option.
	** Defaults to 1.
	** 
	**   mongodb://example.com/puppies?minPoolSize=50
	const Int 	minPoolSize	:= 1

	** The maximum number of database connections this pool is allowed open.
	** This is the maximum number of concurrent users you expect your application to have.
	** 
	** Set via the [maxPoolSize]`https://docs.mongodb.org/manual/reference/connection-string/#urioption.maxPoolSize` connection string option.
	** Defaults to 10.
	** 
	**   mongodb://example.com/puppies?maxPoolSize=10
	const Int 	maxPoolSize	:= 10
	
	** The maximum time a thread can wait for a connection to become available.
	** 
	** Set via the [maxPoolSize]`https://docs.mongodb.org/manual/reference/connection-string/#urioption.waitQueueTimeoutMS` connection string option.
	** Defaults to 15 seconds.
	** 
	**   mongodb://example.com/puppies?waitQueueTimeoutMS=10
	const Duration	waitQueueTimeout := 15sec

	** If specified, this is the time to attempt a connection before timing out.
	** If 'null' (the default) then a system timeout is used.
	** 
	** Set via the [connectTimeoutMS]`https://docs.mongodb.org/manual/reference/connection-string/#urioption.connectTimeoutMS` connection string option.
	** 
	**   mongodb://example.com/puppies?connectTimeoutMS=2500
	** 
	** Equates to `inet::SocketOptions.connectTimeout`.
	const Duration? connectTimeout
	
	** If specified, this is the time to attempt a send or receive on a socket before the attempt times out.
	** 'null' (the default) indicates an infinite timeout.
	** 
	** Set via the [socketTimeoutMS]`https://docs.mongodb.org/manual/reference/connection-string/#urioption.socketTimeoutMS` connection string option.
	** 
	**   mongodb://example.com/puppies?socketTimeoutMS=2500
	** 
	** Equates to `inet::SocketOptions.receiveTimeout`.
	const Duration? socketTimeout

	** When the connection pool is shutting down, this is the amount of time to wait for all connections for close before they are forcibly closed.
	** 
	** Defaults to '2sec'. 
	const Duration? shutdownTimeout	:= 2sec
	
	** Specifies an SSL connection. Set to 'true' for Atlas databases.
	** 
	** Defaults to 'false'. 
	const Bool ssl := false
	
	@NoDoc
	override Str? authSource() { defaultDatabase }

	// used to test the backoff func
	internal const |Range->Int|	randomFunc	:= |Range r->Int| { r.random }
	internal const |Duration| 	sleepFunc	:= |Duration napTime| { Actor.sleep(napTime) }
	
	** Create a 'ConnectionManager' from a [Mongo Connection URL]`http://docs.mongodb.org/manual/reference/connection-string/`.
	** If user credentials are supplied, they are used as default authentication for each connection.
	** 
	**   conMgr := ConnectionManagerPooled(ActorPool(), `mongodb://localhost:27017`)
	** 
	** The following URL options are supported:
	**  - [minPoolSize]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.minPoolSize`
	**  - [maxPoolSize]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.maxPoolSize`
	**  - [waitQueueTimeoutMS]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.waitQueueTimeoutMS`
	**  - [connectTimeoutMS]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.connectTimeoutMS`
	**  - [socketTimeoutMS]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.socketTimeoutMS`
	**  - [w]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.w`
	**  - [wtimeoutMS]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.wtimeoutMS`
	**  - [journal]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.journal`
	**  - [ssl]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.ssl`
	**  - [tls]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.tls`
	**  - [authSource]`https://docs.mongodb.com/manual/reference/connection-string/#urioption.authSource`
	** 
	** URL examples:
	**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
	**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
	** 
	** @see `http://docs.mongodb.org/manual/reference/connection-string/`
	new makeFromUrl(ActorPool actorPool, Uri connectionUrl, |This|? f := null) {
		if (connectionUrl.scheme != "mongodb")
			throw ArgErr(MongoErrMsgs.connectionManager_badScheme(connectionUrl))

		mongoUrl				:= connectionUrl
		this.connectionUrl		 = connectionUrl
		this.connectionState	 = SynchronizedState(actorPool, ConnectionManagerPoolState#)
		this.failOverThread		 = Synchronized(actorPool)
		this.minPoolSize 		 = mongoUrl.query["minPoolSize"]?.toInt ?: minPoolSize
		this.maxPoolSize 		 = mongoUrl.query["maxPoolSize"]?.toInt ?: maxPoolSize
		waitQueueTimeoutMs		:= mongoUrl.query["waitQueueTimeoutMS"]?.toInt
		connectTimeoutMs		:= mongoUrl.query["connectTimeoutMS"]?.toInt
		socketTimeoutMs 		:= mongoUrl.query["socketTimeoutMS"]?.toInt
		w						:= mongoUrl.query["w"]
		wtimeoutMs		 		:= mongoUrl.query["wtimeoutMS"]?.toInt
		journal			 		:= mongoUrl.query["journal"]?.toBool
		ssl				 		 =(mongoUrl.query["tls"]?.toBool ?: mongoUrl.query["ssl"]?.toBool) ?: false

		if (minPoolSize < 0)
			throw ArgErr(MongoErrMsgs.connectionManager_badInt("minPoolSize", "zero", minPoolSize, mongoUrl))
		if (maxPoolSize < 1)
			throw ArgErr(MongoErrMsgs.connectionManager_badInt("maxPoolSize", "one", maxPoolSize, mongoUrl))
		if (minPoolSize > maxPoolSize)
			throw ArgErr(MongoErrMsgs.connectionManager_badMinMaxConnectionSize(minPoolSize, maxPoolSize, mongoUrl))		
		if (waitQueueTimeoutMs != null && waitQueueTimeoutMs < 0)
			throw ArgErr(MongoErrMsgs.connectionManager_badInt("waitQueueTimeoutMS", "zero", waitQueueTimeoutMs, mongoUrl))
		if (connectTimeoutMs != null && connectTimeoutMs < 0)
			throw ArgErr(MongoErrMsgs.connectionManager_badInt("connectTimeoutMS", "zero", connectTimeoutMs, mongoUrl))
		if (socketTimeoutMs != null && socketTimeoutMs < 0)
			throw ArgErr(MongoErrMsgs.connectionManager_badInt("socketTimeoutMS", "zero", socketTimeoutMs, mongoUrl))
		if (wtimeoutMs != null && wtimeoutMs < 0)
			throw ArgErr(MongoErrMsgs.connectionManager_badInt("wtimeoutMS", "zero", wtimeoutMs, mongoUrl))

		if (waitQueueTimeoutMs != null)
			waitQueueTimeout = (waitQueueTimeoutMs * 1_000_000).toDuration
		if (connectTimeoutMs != null)
			connectTimeout = (connectTimeoutMs * 1_000_000).toDuration
		if (socketTimeoutMs != null)
			socketTimeout = (socketTimeoutMs * 1_000_000).toDuration

		// authSource trumps defaultauthdb 
		database := mongoUrl.query["authSource"]?.trimToNull ?: mongoUrl.pathStr.trimToNull
		username := mongoUrl.userInfo?.split(':')?.getSafe(0)?.trimToNull
		password := mongoUrl.userInfo?.split(':')?.getSafe(1)?.trimToNull
		
		if ((username == null).xor(password == null))
			throw ArgErr(MongoErrMsgs.connectionManager_badUsernamePasswordCombo(username, password, mongoUrl))

		if (database != null && database.startsWith("/"))
			database = database[1..-1].trimToNull
		if (username != null && password != null && database == null)
			database = "admin"
		if (username == null && password == null)	// a default database has no meaning without credentials
			database = null
		
		defaultDatabase = database
		defaultUsername = username
		defaultPassword = password
		
		writeConcern := Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("j", false)
		if (w != null)
			writeConcern["w"] = Int.fromStr(w, 10, false) != null ? w.toInt : w
		if (wtimeoutMs != null)
			writeConcern["wtimeout"] = wtimeoutMs
		if (journal != null)
			writeConcern["j"] = journal
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
		query.remove("ssl")
		query.remove("tls")
		query.remove("authSource")
		query.each |val, key| {
			log.warn("Unknown option in Mongo connection URL: ${key}=${val} - ${mongoUrl}")
		}
		
		// allow the it-block to override the default settings
		// no validation occurs - only used for testing.
		f?.call(this)
	}
	
	** Creates the initial pool and establishes 'minPoolSize' connections with the server.
	** 
	** If a connection URL to a replica set is given (a connection URL with multiple hosts) then 
	** the hosts are queried to find the primary. The primary is currently used for all read and 
	** write operations. 
	override ConnectionManager startup() {
		shutdownLock.check
		if (startupLock.locked)
			return this
		startupLock.lock
		
		huntThePrimary

		// connect x times
		pool := TcpConnection[,]
		minPoolSize.times { pool.push(checkOut) }
		minPoolSize.times { checkIn(pool.pop) }
		
		return this
	}

	** Makes a connection available to the given function.
	** What ever is returned from the func is returned from the method.
	** 
	** If all connections are currently in use, a truncated binary exponential backoff algorithm 
	** is used to wait for one to become free. If, while waiting, the duration specified in 
	** 'waitQueueTimeout' expires then a 'MongoErr' is thrown.
	** 
	** All leased connections are authenticated against the default credentials.
	override Obj? leaseConnection(|Connection->Obj?| c) {
		if (!startupLock.locked)
			throw MongoErr(MongoErrMsgs.connectionManager_notStarted)
		shutdownLock.check

		connection := checkOut
		try {
			return c(connection)
			
		} catch (MongoOpErr e) {
			err := e as Err
			connection.close

			// that shitty MongoDB Atlas doesn't tell us when the master has changed 
			// instead we just get errors when we attempt to read the reply

			// if the master URL has changed, then we've already found a new master!
			if (connection.mongoUrl != mongoUrl)
				throw err

			// if we're still connected to the same master, lets play huntThePrimary!
			failOver

			// even though Hunt the Primary succeeded, we still need to report the original error!
			// it would be cool to just call the "c" func again, but we can't be sure it's idempotent
			throw err

		} catch (MongoErr e) {
			err := e as Err
			connection.close

			if (!err.msg.contains("MongoDB says: not master"))
				throw err

			// if the master URL has changed, then we've already found a new master!
			if (connection.mongoUrl != mongoUrl)
				throw err

			// if we're still connected to the same master, lets play huntThePrimary!
			failOver
				
			// even though Hunt the Primary succeeded, we still need to report the original error!
			// it would be cool to just call the "c" func again, but we can't be sure it's idempotent
			throw err
			
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
		if (!startupLock.locked)
			return this
		shutdownLock.lock
		
		closeFunc := |->Bool?| {
			waitingOn := connectionState.sync |ConnectionManagerPoolState state -> Int| {
				while (!state.checkedIn.isEmpty) {
					state.checkedIn.removeAt(0).close 
				}
				return state.checkedOut.size
			}
			if (waitingOn > 0)
				log.info("Waiting for ${waitingOn} connections to close on ${mongoUrl}...")
			return waitingOn > 0 ? null : true
		}
		
		allClosed := backoffFunc(closeFunc, shutdownTimeout) ?: false

		if (!allClosed) {
			// too late, they've had their chance. Now everybody dies.
			connectionState.async |ConnectionManagerPoolState state| {
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
	
	** Returns the number of pooled connections currently in use.
	Int noOfConnectionsInUse() {
		connectionState.sync |ConnectionManagerPoolState state->Int| {
			state.checkedOut.size
		}		
	}

	** Returns the number of connections currently in the pool.
	Int noOfConnectionsInPool() {
		connectionState.sync |ConnectionManagerPoolState state->Int| {
			state.checkedOut.size + state.checkedIn.size
		}
	}
	
	** (Advanced)
	** Searches the replica set for the Master node and instructs all new connections to connect to it.
	** Throws 'MongoErr' if a primary can not be found. 
	** 
	** This method should be followed with a call to 'emptyPool()'.  
	Void huntThePrimary() {
		mongoUrl := HuntThePrimary(connectionUrl, ssl).huntThePrimary

		mongoUrlRef.val = mongoUrl

		// set our connection factory
		connectionState.sync |ConnectionManagerPoolState state| {
			state.connectionFactory = |->Connection| {
				socket := ssl ? TcpSocket.makeTls : TcpSocket.make
				socket.options.connectTimeout = connectTimeout
				socket.options.receiveTimeout = socketTimeout
				return TcpConnection(socket).connect(IpAddr(mongoUrl.host), mongoUrl.port) {
					it.mongoUrl = mongoUrlRef.val
				}
			} 
		}
	}
	
	** (Advanced)
	** Closes all un-leased connections in the pool, and flags all leased connections to close 
	** themselves after use. Use to migrate connections to new host / master.
	Void emptyPool() {
		connectionState.sync |ConnectionManagerPoolState state| {
			while (!state.checkedIn.isEmpty) {
				state.checkedIn.removeAt(0).close 
			}
			state.checkedOut.each { it.forceCloseOnCheckIn = true }
		}
	
		// re-connect x times
		pool := TcpConnection[,]
		minPoolSize.times { pool.push(checkOut) }
		minPoolSize.times { checkIn(pool.pop) }
	}
	
	private Void failOver() {
		// no need to have 3 threads huntingThePrimary at the same time!
		if (failingOverRef.val == true)
			return

		// it doesn't matter if a race condition means we play huntThePrimary twice in succession
		failOverThread.async |->| {
			failingOverRef.val = true
			try	{
				huntThePrimary
				emptyPool
				
				// we're an unsung hero - we've established a new master connection and nobody knows! 
				
			} catch (Err err) {
				log.warn("Could not find new Master", err)

			} finally {
				failingOverRef.val = false
			}
		}
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

	private TcpConnection checkOut() {
		connectionFunc := |Duration totalNapTime->TcpConnection?| {
			con := connectionState.sync |ConnectionManagerPoolState state->Unsafe?| {
				while (!state.checkedIn.isEmpty) {
					connection := state.checkedIn.pop
					
					// check the connection is still alive - the server may have closed it during a fail over
					if (!connection.isClosed) {
						state.checkedOut.push(connection)
						return Unsafe(connection)
					}
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
				log.warn("All ${maxPoolSize} are in use, waiting for one to become free on ${mongoUrl}...")

			return con
		}

		connection	:= null as TcpConnection
		ioErr		:= null as Err
		try connection = backoffFunc(connectionFunc, waitQueueTimeout)
		
		// sys::IOErr: Could not connect to MongoDB at `dsXXXXXX-a0.mlab.com:59296` - java.net.ConnectException: Connection refused
		catch (IOErr ioe)
			ioErr = ioe

		if (connection == null || ioErr != null) {
			if (noOfConnectionsInUse == maxPoolSize)
				throw MongoErr("Argh! No more connections! All ${maxPoolSize} are in use!")
			
			// it would appear the database is down ... :(			
			// so lets kick off a game of huntThePrimary in the background ...
			failOver

			// ... and report an error - 'cos we can't wait longer than 'waitQueueTimeout'
			throw ioErr ?: MongoErr("Argh! Can not connect to Master! All ${maxPoolSize} are in use!")
		}
		
		// ensure all connections that are initially leased are authenticated as the default user
		// specifically do the check here so you can always *brute force* an authentication on a connection
		if (defaultDatabase != null && connection.authentications[defaultDatabase] != defaultUsername)
			connection.authenticate(defaultDatabase, defaultUsername, defaultPassword)
		
		return connection
	}
	
	private Void checkIn(TcpConnection connection) {
		unsafeConnection := Unsafe(connection)
		// call sync() to make sure this thread checks in before it asks for a new one
		connectionState.sync |ConnectionManagerPoolState state| {
			conn := (TcpConnection) unsafeConnection.val
			state.checkedOut.removeSame(conn)
			
			// make sure we don't save stale connections
			if (!conn.isClosed)
				// only keep the min pool size
				if (conn.forceCloseOnCheckIn || state.checkedIn.size >= minPoolSize)
					conn.close
				else
					state.checkedIn.push(conn)
		}
	}
}

internal class ConnectionManagerPoolState {
	TcpConnection[]		checkedIn	:= [,]
	TcpConnection[]		checkedOut	:= [,]
	|->TcpConnection|?	connectionFactory
}
