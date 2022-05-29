using concurrent::ActorPool
using concurrent::Future

** Manages a pool of connections to a MongoDB instance.
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
** See `https://www.mongodb.com/docs/manual/reference/connection-string/` for details.
** 
** On 'startup()' the hosts are queried to find the primary / master node. 
** All read and write operations are performed on the primary node.
** 
** When a connection to the master node is lost, all hosts are re-queried to find the new master.
** 
** Note this connection manager *is* safe for multi-threaded / web-application use.
const class MongoConnMgr {
	
	** The log instance used to report warnings.
	Log log() { pool.log }
	
	** Basic details of where this 'ConnectionManager' connects to, for debugging purposes.
	** When connecting to replica sets, this should indicate the primary. 
	** 
	** It *should not* contain any user credentials and *should* be safe to log. 
	** 
	** This value is unavailable (returns 'null') until 'startup()' is called. 
	Uri? mongoUrl() { pool.mongoUrl }
	
	** The parsed Mongo Connection URL.	
	MongoConnUrl mongoConnUrl() { pool.mongoConnUrl }
	
	** Creates the initial pool and establishes 'minPoolSize' connections with the server.
	** 
	** If a connection URL to a replica set is given (a connection URL with multiple hosts) then 
	** the hosts are queried to find the primary. The primary is currently used for all read and 
	** write operations. 
	This startup() { pool.startup; return this }

	** Makes a connection available to the given function.
	** 
	** What ever is returned from the func is returned from the method.
	** 
	** If all connections are currently in use, a truncated binary exponential backoff algorithm 
	** is used to wait for one to become free. If, while waiting, the duration specified in 
	** 'waitQueueTimeout' expires then a 'MongoErr' is thrown.
	** 
	** All leased connections are authenticated against the default credentials
	** 
	** Any 'IOErrs' thrown in the fn are assumed to be networking errors, and invoke a topology 
	** rescan and a Master failover.
	Obj? leaseConn(|MongoConn->Obj?| c) { pool.leaseConn(c) }

	** Runs the given 'fn' in a Mongo multi-cmd, multi-collection, transaction. 
	** Should the 'fn' complete normally, the transaction is committed.
	** If the 'fn' throws an Err, the transaction is aborted / rolled back.
	** 
	** pre>
	** syntax: fantom
	** runInTxn([
	**   "readConcern"    : [...],
	**   "writeConcern"   : [...],
	**   "timeoutMS"      : 10_000,
	** ]) {
	**   ...
	**   // do some Mongo stuff
	**   ...
	** }
	** <pre
	** 
	** The passed function **MUST** be **idempotent** - as it will be re-executed on transient 
	** MongoDB server errors.
	** 
	** Note: The obj passed to 'fn' is undefined and should not be used.
	Void runInTxn([Str:Obj?]? txnOpts, |Obj?| txnFn) { pool.runInTxn(this, txnOpts, txnFn) }
	
	** Closes all MongoDB connections.
	This shutdown() { pool.shutdown; return this }

	** Sets the log level to 'debug' to log all cmd request and responses. 
	virtual This setDebug(Bool debugOn := true) {
		log.level = debugOn ? LogLevel.debug : LogLevel.info
		return this
	}
	
	** Returns properties and statistics about this connection manager.
	** 
	** pre>
	** syntax: fantom
	** stats() // --> [
	**   "mongoUrl"        : `mongodb://localhost:27017/dbName`,
	**   "hosts"           : ["localhost", "otherhost"]
	**   "maxWireVer"      : 7,
	**   "compression"     : ["zlib", "snappy"]
	**   "sessionTimeout"  : 30min,
	**   "numConnsInUse"   : 3,
	**   "numConnsInPool"  : 7,
	**   "primaryFound"    : true,
	**   "
	** ]
	** 
	** The keys and data returned are for debug info only and are not guaranteed to exist in 
	** future driver versions.
	Str:Obj? props() { pool.props }
	
	** (Advanced)
	** Returns 'true' if the server type is standalone and does not declare any hosts.
	** Required info for transactions and retryable writes.
	@NoDoc	// advanced
	Bool isStandalone() { pool.isStandalone }

	** (Advanced)
	** Authenticates the given connection against the Master, with credentials given via the Mongo connection URL.
	@NoDoc	// advanced
	Void authenticateConn(MongoConn conn) { pool.authenticateConn(conn) }
	
	** (Advanced)
	** To be called on a network 'IOErr'.
	** 
	** Searches the replica set for the Master node - throws 'MongoErr' if the primary can not be found. 
	** 
	** The connection pool is then cleared down and all existing connections closed.
	** All new connections will then re-connect to their new Master.
	@NoDoc	// advanced
	Future failOver() { pool.failOver }

	** Creates a pooled Mongo Connection Manager.
	** 
	** URL examples:
	**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
	**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
	** 
	** If user credentials are supplied, they are used as default authentication for each connection. 
	** 
	** See `MongoConnUrl` for connection URL details.
	new make(Uri connectionUrl, Log? log := null, ActorPool? actorPool := null) { 
		this.pool = MongoConnMgrPool(connectionUrl, log)
	}
	
	internal new _forTest(MongoConnMgrPool pool) {
		this.pool = pool
	}
	
	private const MongoConnMgrPool	pool
	
	internal Int failoverCount() {
		pool->failoverCount
	}
}
