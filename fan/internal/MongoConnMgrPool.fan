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
	private const MongoBackoff		backoff					:= MongoBackoff()
	
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
		return failingOverRef.val = failOverThread.async |->Uri| {
			try	{
				oldUrl := this.mongoUrl
				huntThePrimary
				emptyPool
				newUrl := this.mongoUrl
				
				// not sure why this would ever be the case... but let's make sure
				if (newUrl == null)
					throw Err("Could not find new Master")
				
				if (oldUrl != newUrl)
					log.warn("MongoDB Master failed over from $oldUrl to $newUrl")
				
				// we're an unsung hero - we've established a new master connection and nobody knows! 
				return newUrl
				
			} catch (Err err)
				throw IOErr("Could not find new Master", err)

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
		
		allClosed := backoff.backoffFunc(closeFunc, shutdownTimeout, StrBuf()) ?: false

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
		if (isConnected) {
			conn := newMongoConn(mongoUrl)
			try		sessPool.shutdown(conn)
			finally	conn.close
		}

		return this
	}
	
	Str:Obj? props() {
		connectionState.sync |MongoConnMgrPoolState state->Str:Obj?| {
			Str:Obj?[:].with |m| { 
				m.ordered 				= true
				m.add("mongoUrl"		, this.mongoUrl)
				m.add("primaryFound"	, this.primaryDetails != null)
				m.add("maxWireVer"		, this.primaryDetails?.maxWireVer)
				m.add("hosts"			, this.primaryDetails?.hosts)
				m.add("compression"		, this.primaryDetails?.compression)
				m.add("sessionTimeout"	, this.primaryDetails?.sessionTimeout)
				m.add("numConns"		, state.checkedOut.size + state.checkedIn.size)
				m.add("numConnsInUse"	, state.checkedOut.size)
				
				// add non-default conn url params
				baseUrl := MongoConnUrl.fromUrl(`mongodb://wotever`)
				ignore  := "connectionUrl dbName authMechs mongoCreds".split
				MongoConnUrl#.fields.each |field| {
					if (ignore.contains(field.name) == false && field.get(this.mongoConnUrl) != field.get(baseUrl))
						m.add(field.name, field.get(this.mongoConnUrl))
				}
	
				MongoConn[,]
					.addAll(state.checkedOut)
					.addAll(state.checkedIn)
					.sort |c1, c2| { c1._id <=> c2._id }
					.each |conn| { m.addAll(conn._props) }
			}
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
		if (primaryDetails.sessionTimeout != null)
			sessPool.sessionTimeout = primaryDetails.sessionTimeout

		// set our connection factory
		mongoUrl := primaryDetails.mongoUrl
		connectionState.sync |MongoConnMgrPoolState state| {
			state.connFactory = |->MongoConn| {
				return newMongoConn(mongoUrl) {
					it._mongoUrl				= mongoUrl
					it._compressor				= primaryDetails.compression.first
					it._zlibCompressionLevel	= this.mongoConnUrl.zlibCompressionLevel
				}
			} 
		}

		this.primaryDetails = primaryDetails
	}
	
	virtual MongoConn newMongoConn(Uri mongoUrl) {
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
		connectionFunc := |Duration totalNapTime, Unsafe msgRef->MongoConn?| {
			conn := connectionState.sync |MongoConnMgrPoolState state->Unsafe?| {
				msgBuf := (StrBuf) msgRef.val
				msgBuf.join("Checked in pool size: ${state.checkedIn.size}", "\n")
				while (state.checkedIn.size > 0) {
					conn := state.checkedIn.pop
					cid  := conn._id.toHex(4)
					msgBuf.join("Conn ${cid} - Checked out", "\n")

					// check the connection is still alive - the server may have closed it during a fail over
					// close and discard any old connections
					if (conn.isClosed) {
						msgBuf.join("Conn ${cid} - Conn is closed (may have been closed during a failover?)", "\n")
						conn.close
					} else
					
					if (conn._isStale(mongoConnUrl.maxIdleTime)) {
						msgBuf.join("Conn ${cid} - Conn has been lingering for more than ${mongoConnUrl.maxIdleTime.toLocale}", "\n")
						conn.close
					} else
					
					{
						conn._lingeringSince = null
						state.checkedOut.push(conn)
						return Unsafe(conn)
					}
				}

				msgBuf.join("Checked out pool size: ${state.checkedOut.size}", "\n")
				if (state.checkedOut.size >= mongoConnUrl.maxPoolSize) {
					msgBuf.join("Checked out pool is full", "\n")
					return null
				}
				
				// create a new connection
				msgBuf.join("Creating new connection", "\n")
				connection := state.connFactory()
				state.checkedOut.push(connection)
				return Unsafe(connection)
			}?->val
			
			// let's not swamp the logs the first time we can't connect
			// 1.5 secs gives at least 6 connection attempts
			if (conn == null && totalNapTime > 1.5sec)
				log.warn("All ${mongoConnUrl.maxPoolSize} are in use, waiting for one to become free on ${mongoUrl}...")
			return conn
		}

		connection	:= null as MongoConn
		ioErr		:= null as Err
		msgBuf		:= StrBuf()
		try connection = backoff.backoffFunc(connectionFunc, mongoConnUrl.waitQueueTimeout, msgBuf)

		// sys::IOErr: Could not connect to MongoDB at `dsXXXXXX-a0.mlab.com:59296` - java.net.ConnectException: Connection refused
		catch (IOErr ioe)
			ioErr = ioe

		if (connection == null || ioErr != null) {
			detail := msgBuf.toStr.splitLines.join("\n") { " - ${it}" }
			if (noOfConnectionsInUse >= mongoConnUrl.maxPoolSize)
				throw Err("Argh! No more Mongo connections! All ${mongoConnUrl.maxPoolSize} are in use!\n${detail}", ioErr)
			
			// it would appear the database is down ... :(			
			// so lets kick off a game of huntThePrimary in the background ...
			failOver

			// ... and report an error - 'cos we can't wait longer than 'waitQueueTimeout'
			throw ioErr ?: Err("Argh! Can not connect to Mongo Master! All ${mongoConnUrl.maxPoolSize} are in use!\n${detail}")
		}

		// even if we're not authenticated (yet), the Conn is still checked out
		// this ensures Conns are flagged as "in use" when we have MongoDB connection issues
		connection._onCheckOut

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
			
			// let the conn clean up after itself
			conn._onCheckIn

			// check the session back into the pool for future reuse
			// if the session has already been detached, then conn.detachSess() will return null
			sessPool.checkin(conn._detachSession, true)
			
			// make sure we don't save stale connections
			if (conn.isClosed)
				return
				
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
			// despite the lingering, as long as the pool is full, the conn will not be removed
			conn._lingeringSince	= Duration.now

			state.checkedIn.push(conn)
		}
	}
}

internal class MongoConnMgrPoolState {
	MongoConn[]		checkedIn	:= [,]
	MongoConn[]		checkedOut	:= [,]
	|->MongoConn|?	connFactory
}
