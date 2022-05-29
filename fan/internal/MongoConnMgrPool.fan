using concurrent::AtomicBool
using concurrent::AtomicRef
using concurrent::Actor
using concurrent::ActorPool
using concurrent::Future
using afConcurrent::Synchronized
using afConcurrent::SynchronizedState
using inet::IpAddr
using inet::TcpSocket

@NoDoc	// advanced use only
internal const class MongoConnMgrPool : MongoConnMgr {
	override const Log				log
	private const AtomicBool		hasStarted				:= AtomicBool()
	private const AtomicBool		hasShutdown				:= AtomicBool()
	private const AtomicRef 		failingOverRef			:= AtomicRef(null)
	private const AtomicBool 		isConnectedToMasterRef	:= AtomicBool(false)
	private const AtomicBool 		isStandaloneRef			:= AtomicBool(true)		// disable transactions until we know they're accepted
	private const Synchronized		failOverThread
	private const SynchronizedState connectionState
	
	// todo this should (probably) live in the MongoClient
	// having it here overloads the responsibility of a ConnMgr
	private const MongoSessPool		sessPool	

	** The host name of the MongoDB server this 'ConnectionManager' connects to.
	** When connecting to replica sets, this will indicate the primary.
	** 
	** This value is unavailable (returns 'null') until 'startup()' is called. 
	override Uri? mongoUrl() { mongoUrlRef.val }
	private const AtomicRef mongoUrlRef := AtomicRef(null)
	
	** The parsed Mongo Connection URL.	
	const MongoConnUrl mongoConnUrl

	override Str? dbName() {
		mongoConnUrl.database
	}
	
	** When the connection pool is shutting down, this is the amount of time to wait for all 
	** connections for close before they are forcibly closed.
	** 
	** Defaults to '2sec'. 
	const Duration? shutdownTimeout	:= 5sec
	
	// used to test the backoff func
	internal const |Range->Int|	randomFunc	:= |Range r->Int| { r.random }
	internal const |Duration| 	sleepFunc	:= |Duration napTime| { Actor.sleep(napTime) }
	
	new make(Uri connectionUrl, Log? log := null, ActorPool? actorPool := null, |This|? fn := null) {
			 actorPool			= actorPool ?: ActorPool() { it.name = "afMongo.connMgrPool"; it.maxThreads = 5 }
		this.connectionState	= SynchronizedState(actorPool, MongoConnMgrPoolState#)
		this.mongoConnUrl		= MongoConnUrl(connectionUrl)
		this.failOverThread		= connectionState.lock
		this.sessPool			= MongoSessPool(actorPool)
		this.log				= log ?: MongoConnMgrPool#.pod.log
		
		// for backoff tests
		fn?.call(this)
	}
	
	override [Str:Obj?]? writeConcern() {
		mongoConnUrl.writeConcern
	}
	
	override Bool retryReads() {
		mongoConnUrl.retryReads
	}

	override Bool retryWrites() {
		mongoConnUrl.retryWrites
	}

	override Bool isStandalone() {
		isStandaloneRef.val
	}

	override This startup() {
		if (hasShutdown.val == true)
			throw Err("Connection Pool has been shutdown")
		starting := hasStarted.compareAndSet(false, true)
		if (starting == false)
			return this
		
		huntThePrimary

		// connect x times
		pool := MongoConn[,]
		mongoConnUrl.minPoolSize.times { pool.push(checkOut) }
		mongoConnUrl.minPoolSize.times { checkIn(pool.pop) }
		
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
	override Obj? leaseConn(|MongoConn->Obj?| c) {
		if (hasStarted.val == false)
			throw Err("ConnectionManager has not started")
		if (hasShutdown.val == true)
			throw Err("Connection Pool has been shutdown")

		conn := checkOut
		try {
			return c(conn)
	
		// all this error handling is because there's no guarantee that this is called from MongoOp
		} catch (IOErr err) {
			conn.close

			// that shitty MongoDB Atlas doesn't tell us when the master has changed 
			// instead we just get IOErrs when we attempt to read the reply

			// if the master URL has changed, then we've already found a new master!
			if (conn._mongoUrl != mongoUrl)
				throw err

			// if we're still connected to the same master, lets play huntThePrimary!
			// it doesn't matter if the MongoOp retry already kicked it off
			failOver

			// even though Hunt the Primary succeeded, we still need to report the original error!
			// it would be cool to just call the "c" func again, but we can't be sure it's idempotent
			// and retryable writes take care of most of the chaff
			throw err
			
		} catch (MongoErr err) {
			// a MongoDB error means the comms is fine - no need to close the connection and re-auth 
			throw err

		} catch (Err err) {
			// if something dies, kill the connection.
			// we may have died part way through talking with the server meaning our communication 
			// protocols are out of sync - rendering any future use of the connection useless.
			conn.close
			throw err

		} finally
			checkIn(conn)
	}
	
	override Void runInTxn([Str:Obj?]? txnOpts, |Obj| fn) {
		sessPool.checkout.runInTxn(this, txnOpts, fn)
	}
	
	override Future failOver() {
		// no need to have 3 threads huntingThePrimary at the same time!
		future := failingOverRef.val
		if (future != null)
			return future
		
		log.warn("Failing over. Re-scanning network topology for new master...")

		// it doesn't matter if a race condition means we play huntThePrimary twice in succession
		return failingOverRef.val = failOverThread.async |->| {
			isConnectedToMasterRef.val = false
			try	{
				oldUrl := this.mongoUrl
				huntThePrimary
				emptyPool
				newUrl := this.mongoUrl
				
				if (oldUrl != newUrl)
					log.warn("MongoDB Master failed over from $oldUrl to $newUrl")
				
				// we're an unsung hero - we've established a new master connection and nobody knows! 
				isConnectedToMasterRef.val = true
				
			} catch (Err err)
				log.warn("Could not find new Master", err)

			finally
				failingOverRef.val = null
		}
	}
	
	** Closes all connections. 
	** 
	** Initially waits for 'shutdownTimeout' for connections to finish what they're doing before 
	** they're closed. After that, all open connections are forcibly closed regardless of whether 
	** they're in use or not.
	override This shutdown() {
		if (hasStarted.val == false)
			return this
		shuttingDown := hasShutdown.compareAndSet(false, true)
		if (shuttingDown == false)
			return this
		
		closeFunc := |->Bool?| {
			waitingOn := connectionState.sync |MongoConnMgrPoolState state -> Int| {
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
			connectionState.async |MongoConnMgrPoolState state| {
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
		
		// one last call to the server to end all sessions
		conn := newMongoConn
		try		sessPool.shutdown(conn)
		finally	conn.close

		return this
	}
	
	** Returns the number of pooled connections currently in use.
	Int noOfConnectionsInUse() {
		connectionState.sync |MongoConnMgrPoolState state->Int| {
			state.checkedOut.size
		}		
	}

	** Returns the number of connections currently in the pool.
	Int noOfConnectionsInPool() {
		connectionState.sync |MongoConnMgrPoolState state->Int| {
			state.checkedOut.size + state.checkedIn.size
		}
	}
	
	** Returns 'true' if we'er currently connected to a Master node and can accept write cmds.
	** 
	** Returns 'false' during failovers and games of "Hunt the Primary".
	Bool isConnectedToMaster() {
		isConnectedToMasterRef.val
	}
	
	** (Advanced)
	** Searches the replica set for the Master node and instructs all new connections to connect to it.
	** Throws 'MongoErr' if a primary can not be found. 
	** 
	** This method should be followed with a call to 'emptyPool()'.  
	virtual Void huntThePrimary() {
		hostDetails := MongoSafari(mongoConnUrl, log).huntThePrimary
		
		// save the address of our new Master
		mongoUrl	:= dbName == null ? hostDetails.mongoUrl : hostDetails.mongoUrl.plusSlash.plusName(dbName) 
		mongoUrlRef.val	= mongoUrl
		
		// transactions are not allowed on standalone instances
		isStandaloneRef.val	= hostDetails.isStandalone

		// keep track of the new logical session timeout
		sessPool.sessionTimeout = hostDetails.sessionTimeout

		// set our connection factory
		connectionState.sync |MongoConnMgrPoolState state| {
			state.connFactory = |->MongoConn| {
				return newMongoConn {
					it._mongoUrl				= mongoUrl
					it._compressor				= hostDetails.compression.first
					it._zlibCompressionLevel	= this.mongoConnUrl.zlibCompressionLevel
				}
			} 
		}

		isConnectedToMasterRef.val = true
	}
	
	virtual MongoConn newMongoConn() {
		MongoTcpConn(newSocket, mongoConnUrl.tls, log, sessPool).connect(mongoUrl.host, mongoUrl.port)
	}
	
	** Retain backwards compatibility with all recent versions of Fantom.
	private TcpSocket newSocket() {
		socket	 := null as TcpSocket
		oldSkool := Pod.find("inet").version < Version("1.0.77")
		if (oldSkool) {
			socket = mongoConnUrl.tls ? TcpSocket#.method("makeTls").call : TcpSocket#.method("make").call
			socket->options->connectTimeout = mongoConnUrl.connectTimeout
			socket->options->receiveTimeout = mongoConnUrl.socketTimeout
		}
		else {
			config := Method.findMethod("inet::SocketConfig.cur").call->copy(Field.makeSetFunc([
				Field.findField("inet::SocketConfig.connectTimeout") : mongoConnUrl.connectTimeout,
				Field.findField("inet::SocketConfig.receiveTimeout") : mongoConnUrl.socketTimeout,
			]))
			socket = TcpSocket#.method("make").call(config)
			if (mongoConnUrl.tls)
				socket = socket->upgradeTls
		}
		return socket
	}
	
	** (Advanced)
	** Closes all un-leased connections in the pool, and flags all leased connections to close 
	** themselves after use. Use to migrate connections to new host / master.
	Void emptyPool() {
		connectionState.sync |MongoConnMgrPoolState state| {
			while (!state.checkedIn.isEmpty) {
				state.checkedIn.removeAt(0).close 
			}
			state.checkedOut.each { it._forceCloseOnCheckIn = true }
		}
		// re-connect x times
		pool := MongoConn[,]
		mongoConnUrl.minPoolSize.times { pool.push(checkOut) }
		mongoConnUrl.minPoolSize.times { checkIn(pool.pop) }
	}

	private MongoConn checkOut() {
		connectionFunc := |Duration totalNapTime->MongoConn?| {
			conn := connectionState.sync |MongoConnMgrPoolState state->Unsafe?| {
				while (state.checkedIn.size > 0) {
					conn := state.checkedIn.pop
					// check the connection is still alive - the server may have closed it during a fail over
					if (conn.isClosed == false && conn._isStale(mongoConnUrl.maxIdleTime) == false) {
						conn._lingeringSince = null
						state.checkedOut.push(conn)
						return Unsafe(conn)
					}
	
					// close and discard any old connections
					conn.close
				}

				// create a new connection
				if (state.checkedOut.size < mongoConnUrl.maxPoolSize) {
					connection := state.connFactory()
					state.checkedOut.push(connection)
					return Unsafe(connection)
				}
				return null
			}?->val
			
			// let's not swamp the logs the first time we can't connect
			// 1.5 secs gives at least 6 connection attempts
			if (conn == null && totalNapTime > 1.5sec)
				log.warn("All ${mongoConnUrl.maxPoolSize} are in use, waiting for one to become free on ${mongoUrl}...")
			return conn
		}

		connection	:= null as MongoConn
		ioErr		:= null as Err
		try connection = backoffFunc(connectionFunc, mongoConnUrl.waitQueueTimeout)
		
		// sys::IOErr: Could not connect to MongoDB at `dsXXXXXX-a0.mlab.com:59296` - java.net.ConnectException: Connection refused
		catch (IOErr ioe)
			ioErr = ioe

		if (connection == null || ioErr != null) {
			if (noOfConnectionsInUse >= mongoConnUrl.maxPoolSize)
				throw Err("Argh! No more Mongo connections! All ${mongoConnUrl.maxPoolSize} are in use!")
			
			// it would appear the database is down ... :(			
			// so lets kick off a game of huntThePrimary in the background ...
			failOver

			// ... and report an error - 'cos we can't wait longer than 'waitQueueTimeout'
			throw ioErr ?: Err("Argh! Can not connect to Mongo Master! All ${mongoConnUrl.maxPoolSize} are in use!")
		}
		
		// ensure all connections are authenticated
		authenticateConn(connection)
	
		return connection
	}
	
	override Void authenticateConn(MongoConn conn) {
		mongoCreds := mongoConnUrl.mongoCreds
		if (mongoCreds != null && conn._isAuthenticated == false) {
			// Note - Sessions CAN NOT be used if a conn has multiple authentications
			mongoConnUrl.authMechs[mongoCreds.mechanism].authenticate(conn, mongoCreds)
			((MongoConn) conn)._isAuthenticated = true
		}
	}
	
	private Void checkIn(MongoConn connection) {
		unsafeConnection := Unsafe(connection)
		// call sync() to make sure this thread checks in before it asks for a new one
		connectionState.sync |MongoConnMgrPoolState state| {
			conn := (MongoConn) unsafeConnection.val
			state.checkedOut.removeSame(conn)

			// check the session back into the pool for future reuse
			// if the session has already been detached, then conn.detachSess() will return null
			sessPool.checkin(conn._detachSession, true)
			
			// make sure we don't save stale connections
			if (!conn.isClosed) {
				
				if (conn._forceCloseOnCheckIn) {
					conn.close
					return
				}
	
				// discard any stored stale conns (from the bottom of the stack) but keep minPoolSize
				// same as MongoSessPool
				stale := null as MongoConn
				while (state.checkedIn.size >= mongoConnUrl.minPoolSize && (stale = state.checkedIn.first) != null && stale._isStale(mongoConnUrl.maxIdleTime)) {
					state.checkedIn.removeSame(stale)
				}

				// keep the socket open for 10 secs to ease open / close throttling during bursts of activity.
				conn._lingeringSince	= Duration.now

				state.checkedIn.push(conn)
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
}

internal class MongoConnMgrPoolState {
	MongoConn[]		checkedIn	:= [,]
	MongoConn[]		checkedOut	:= [,]
	|->MongoConn|?	connFactory
}
