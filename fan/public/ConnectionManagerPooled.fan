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
	private const Log				log						:= ConnectionManagerPooled#.pod.log
	private const OneShotLock		startupLock				:= OneShotLock("Connection Pool has been started")
	private const OneShotLock		shutdownLock			:= OneShotLock("Connection Pool has been shutdown")
	private const AtomicBool 		failingOverRef			:= AtomicBool(false)
	private const AtomicBool 		isConnectedToMasterRef	:= AtomicBool(false)
	private const Synchronized		failOverThread
	private const SynchronizedState connectionState

	** The host name of the MongoDB server this 'ConnectionManager' connects to.
	** When connecting to replica sets, this will indicate the primary.
	** 
	** This value is unavailable (returns 'null') until 'startup()' is called. 
	override Uri? mongoUrl() { mongoUrlRef.val }
	private const AtomicRef mongoUrlRef := AtomicRef(null)
	
	** The parsed Mongo Connection URL.	
	const MongoConnUrl mongoConnUrl

	** When the connection pool is shutting down, this is the amount of time to wait for all 
	** connections for close before they are forcibly closed.
	** 
	** Defaults to '2sec'. 
	const Duration? shutdownTimeout	:= 2sec
	
	// used to test the backoff func
	internal const |Range->Int|	randomFunc	:= |Range r->Int| { r.random }
	internal const |Duration| 	sleepFunc	:= |Duration napTime| { Actor.sleep(napTime) }
	
	** Create a 'ConnectionManager' from a Mongo Connection URL.
	** If user credentials are supplied, they are used as default authentication for each connection.
	** 
	**   connMgr := ConnectionManagerPooled(ActorPool(), `mongodb://localhost:27017`)
	new makeFromUrl(ActorPool actorPool, Uri connectionUrl, |This|? f := null) {
		this.connectionState	= SynchronizedState(actorPool, ConnectionManagerPoolState#)
		this.mongoConnUrl		= MongoConnUrl(connectionUrl)
		this.failOverThread		= connectionState.lock

		// allow the it-block to override the default settings
		// no validation occurs - only used for testing.
		f?.call(this)
	}
	
	** The default write concern that all write operations should use.
	override [Str:Obj?]? writeConcern() {
		mongoConnUrl.writeConcern
	}
	
	** Creates the initial pool and establishes 'minPoolSize' connections with the server.
	** 
	** If a connection URL to a replica set is given (a connection URL with multiple hosts) then 
	** the hosts are queried to find the primary. The primary is currently used for all read and 
	** write operations. 
	override This startup() {
		shutdownLock.check
		if (startupLock.locked)
			return this
		startupLock.lock
		
		huntThePrimary
		isConnectedToMasterRef.val = true

		// connect x times
		pool := TcpConnection[,]
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
	override Obj? leaseConnection(|Connection->Obj?| c) {
		if (!startupLock.locked)
			throw MongoErr("ConnectionManager has not started")
		shutdownLock.check

		connection := checkOut
		try {
			return c(connection)
			
		} catch (MongoIoErr e) {
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

		} finally
			checkIn(connection)
	}
	
	** Closes all connections. 
	** Initially waits for 'shutdownTimeout' for connections to finish what they're doing before 
	** they're closed. After that, all open connections are forcibly closed regardless of whether 
	** they're in use or not.
	override This shutdown() {
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
	Void huntThePrimary() {
		mongoUrl := HuntThePrimary(mongoConnUrl.connectionUrl, mongoConnUrl.tls).huntThePrimary

		mongoUrlRef.val = mongoUrl
		isConnectedToMasterRef.val = true

		// set our connection factory
		connectionState.sync |ConnectionManagerPoolState state| {
			state.connectionFactory = |->Connection| {
				socket := newSocket
				return TcpConnection(socket).connect(IpAddr(mongoUrl.host), mongoUrl.port) {
					it.mongoUrl = mongoUrl
				}
			} 
		}
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
		connectionState.sync |ConnectionManagerPoolState state| {
			while (!state.checkedIn.isEmpty) {
				state.checkedIn.removeAt(0).close 
			}
			state.checkedOut.each { it.forceCloseOnCheckIn = true }
		}
		// re-connect x times
		pool := TcpConnection[,]
		mongoConnUrl.minPoolSize.times { pool.push(checkOut) }
		mongoConnUrl.minPoolSize.times { checkIn(pool.pop) }
	}
	
	private Void failOver() {
		// no need to have 3 threads huntingThePrimary at the same time!
		if (failingOverRef.val == true)
			return

		// it doesn't matter if a race condition means we play huntThePrimary twice in succession
		failOverThread.async |->| {
			isConnectedToMasterRef.val = false
			failingOverRef.val = true
			try	{
				oldUrl := this.mongoUrl
				huntThePrimary
				emptyPool
				newUrl := this.mongoUrl
				
				log.warn("MongoDB Master failed over from $oldUrl to $newUrl")
				
				// we're an unsung hero - we've established a new master connection and nobody knows! 
				isConnectedToMasterRef.val = true
				
			} catch (Err err) {
				log.warn("Could not find new Master", err)

			} finally {
				failingOverRef.val = false
			}
		}
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

				if (state.checkedOut.size < mongoConnUrl.maxPoolSize) {
					connection := state.connectionFactory()
					state.checkedOut.push(connection)
					return Unsafe(connection)
				}
				return null
			}?->val
			
			// let's not swamp the logs the first time we can't connect
			// 1.5 secs gives at least 6 connection attempts
			if (con == null && totalNapTime > 1.5sec)
				log.warn("All ${mongoConnUrl.maxPoolSize} are in use, waiting for one to become free on ${mongoUrl}...")

			return con
		}

		connection	:= null as TcpConnection
		ioErr		:= null as Err
		try connection = backoffFunc(connectionFunc, mongoConnUrl.waitQueueTimeout)
		
		// sys::IOErr: Could not connect to MongoDB at `dsXXXXXX-a0.mlab.com:59296` - java.net.ConnectException: Connection refused
		catch (IOErr ioe)
			ioErr = ioe

		if (connection == null || ioErr != null) {
			if (noOfConnectionsInUse == mongoConnUrl.maxPoolSize)
				throw MongoErr("Argh! No more connections! All ${mongoConnUrl.maxPoolSize} are in use!")
			
			// it would appear the database is down ... :(			
			// so lets kick off a game of huntThePrimary in the background ...
			failOver

			// ... and report an error - 'cos we can't wait longer than 'waitQueueTimeout'
			throw ioErr ?: MongoErr("Argh! Can not connect to Master! All ${mongoConnUrl.maxPoolSize} are in use!")
		}
		
		// ensure all connections are authenticated
		mongoCreds := mongoConnUrl.mongoCreds
		if (mongoCreds != null && connection.isAuthenticated == false) {
			mongoConnUrl.authMechs[mongoCreds.mechanism].authenticate(connection, mongoCreds)
			connection.isAuthenticated = true
		}
	
		return connection
	}
	
	private Void checkIn(TcpConnection connection) {
		unsafeConnection := Unsafe(connection)
		// call sync() to make sure this thread checks in before it asks for a new one
		connectionState.sync |ConnectionManagerPoolState state| {
			conn := (TcpConnection) unsafeConnection.val
			state.checkedOut.removeSame(conn)

			// make sure we don't save stale connections
			if (!conn.isClosed) {
				
				if (conn.forceCloseOnCheckIn) {
					conn.close
					return
				}
				
				// only keep the min pool size
				if (state.checkedIn.size >= mongoConnUrl.minPoolSize) {
					
					// if there are msgs still to be processed, don't bother closing as we're likely to just be re-opened again
					queueSize := connectionState.lock.actor.queueSize
					if (queueSize == 0) {
						
						// keep the socket open for 30 secs to ease open / close throttling
						
						conn.close
						return
					}
				}

				// else keep the connection alive for re-use
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

internal class ConnectionManagerPoolState {
	TcpConnection[]		checkedIn	:= [,]
	TcpConnection[]		checkedOut	:= [,]
	|->TcpConnection|?	connectionFactory
}
