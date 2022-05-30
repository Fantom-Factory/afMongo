using concurrent::AtomicBool
using concurrent::AtomicRef
using concurrent::ActorPool
using concurrent::Future
using afConcurrent::Synchronized
using afConcurrent::SynchronizedState
using inet::TcpSocket

@NoDoc	// advanced use only
//internal const class MongoConnMgrPool : MongoConnMgr {
internal const class MongoConnMgrPool {
			const Log				log
			const MongoConnUrl		mongoConnUrl
	private const AtomicBool		hasStarted				:= AtomicBool()
	private const AtomicBool		hasShutdown				:= AtomicBool()
	private const AtomicRef 		failingOverRef			:= AtomicRef(null)
	private const AtomicRef 		primaryDetailsRef		:= AtomicRef(null)
	private const Synchronized		failOverThread
	private const SynchronizedState connectionState
	private const MongoBackoff		backoff	:= MongoBackoff()
	
	// todo this should (probably) live in the MongoClient
	// having it here overloads the responsibility of a ConnMgr
	private const MongoSessPool		sessPool	

	private MongoHostDetails? 		primaryDetails {
		get { primaryDetailsRef.val }
		set { primaryDetailsRef.val = it }
	}
	
	** When the connection pool is shutting down, this is the amount of time to wait for all 
	** connections for close before they are forcibly closed.
	** 
	** Defaults to '2sec'. 
	const Duration? shutdownTimeout	:= 5sec
	
	new make(Uri connectionUrl, Log? log := null, ActorPool? actorPool := null) {
			 actorPool			= actorPool	?: ActorPool() { it.name = "afMongo.connMgrPool"; it.maxThreads = 5 }
		this.log				= log		?: MongoConnMgrPool#.pod.log
		this.connectionState	= SynchronizedState(actorPool, MongoConnMgrPoolState#)
		this.mongoConnUrl		= MongoConnUrl(connectionUrl)
		this.failOverThread		= connectionState.lock
		this.sessPool			= MongoSessPool(actorPool)
		this.backoff			= MongoBackoff()
	}
	
	// for testing
	MongoConnMgr mgr() {
		MongoConnMgr(this)
	}

	Uri? mongoUrl() { 
		if (isConnected == false)
			return null
		if (mongoConnUrl.dbName == null)
			return primaryDetails.mongoUrl
		return primaryDetails.mongoUrl.plusSlash.plusName(mongoConnUrl.dbName) 
	}

	virtual Bool isStandalone() {
		primaryDetails?.isStandalone == true
	}

	This startup() {
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
	
	Bool isConnected() {
		primaryDetails?.isPrimary == true
	}

	Obj? leaseConn(|MongoConn->Obj?| c) {
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

			// if the URL has not changed, then we're still connected to the same host
			// so let's look around to see if anything has changed
			// it doesn't matter if the MongoOp retry already kicked it off
			if (conn._mongoUrl == mongoUrl)
				failOver
			
			// even if Hunt the Primary succeeded, we still need to report the original error
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
	
	Void runInTxn(MongoConnMgr connMgr, [Str:Obj?]? txnOpts, |Obj?| fn) {
		sessPool.checkout.runInTxn(connMgr, txnOpts, fn)
	}
	
	virtual Future failOver() {
		// no need to have 3 threads huntingThePrimary at the same time!
		future := failingOverRef.val
		if (future != null)
			return future
		
		log.warn("Failing over. Re-scanning network topology for new master...")

		// it doesn't matter if a race condition means we play huntThePrimary twice in succession
		return failingOverRef.val = failOverThread.async |->| {
			try	{
				oldUrl := this.mongoUrl
				huntThePrimary
				emptyPool
				newUrl := this.mongoUrl
				
				if (oldUrl != newUrl)
					log.warn("MongoDB Master failed over from $oldUrl to $newUrl")
				
				// we're an unsung hero - we've established a new master connection and nobody knows! 
				
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
	This shutdown() {
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
		
		allClosed := backoff.backoffFunc(closeFunc, shutdownTimeout) ?: false

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
	
	Str:Obj? props() {
		connectionState.sync |MongoConnMgrPoolState state->Str:Obj?| {
			[
				"mongoUrl"			: this.mongoUrl,
				"primaryFound"		: this.primaryDetails != null,
				"maxWireVer"		: this.primaryDetails?.maxWireVer,
				"compression"		: this.primaryDetails?.compression,
				"hosts"				: this.primaryDetails?.hosts,
				"sessionTimeout"	: this.primaryDetails?.sessionTimeout,
				"numConnsInUse"		: state.checkedOut.size,
				"numConnsInPool"	: state.checkedOut.size + state.checkedIn.size,
			]
		}
	}
	
	** (Advanced)
	** Searches the replica set for the Master node and instructs all new connections to connect to it.
	** Throws 'MongoErr' if a primary can not be found. 
	** 
	** This method should be followed with a call to 'emptyPool()'.  
	virtual Void huntThePrimary() {
		this.primaryDetails = null

		primaryDetails := MongoSafari(mongoConnUrl, log).huntThePrimary
		
		// keep track of the new logical session timeout
		sessPool.sessionTimeout = primaryDetails.sessionTimeout

		// set our connection factory
		connectionState.sync |MongoConnMgrPoolState state| {
			state.connFactory = |->MongoConn| {
				return newMongoConn {
					it._mongoUrl				= mongoUrl
					it._compressor				= primaryDetails.compression.first
					it._zlibCompressionLevel	= this.mongoConnUrl.zlibCompressionLevel
				}
			} 
		}

		this.primaryDetails = primaryDetails
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
		try connection = backoff.backoffFunc(connectionFunc, mongoConnUrl.waitQueueTimeout)
		
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
	
	private Int noOfConnectionsInUse() {
		connectionState.sync |MongoConnMgrPoolState state->Int| {
			state.checkedOut.size
		}
	}
	
	virtual Void authenticateConn(MongoConn conn) {
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
}

internal class MongoConnMgrPoolState {
	MongoConn[]		checkedIn	:= [,]
	MongoConn[]		checkedOut	:= [,]
	|->MongoConn|?	connFactory
}
