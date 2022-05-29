using concurrent::ActorPool
using concurrent::Future

** (Service) - 
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
const mixin MongoConnMgr {
	
	// TODO Convert MongoConnMgr to a class - maybe merge with MongoConnMgrPool?
	
	** The log instance used to report warnings.
	abstract Log log()
	
	** Basic details of where this 'ConnectionManager' connects to, for debugging purposes.
	** When connecting to replica sets, this should indicate the primary. 
	** 
	** It *should not* contain any user credentials and *should* be safe to log. 
	** 
	** This value is unavailable (returns 'null') until 'startup()' is called. 
	abstract Uri? mongoUrl()
	
	** The parsed Mongo Connection URL.	
	abstract MongoConnUrl mongoConnUrl()
	
	** Returns 'true' if the server type is standalone and does not declare any hosts.
	** Required info for transactions and retryable writes.
	abstract Bool isStandalone()

	** Creates the initial pool and establishes 'minPoolSize' connections with the server.
	** 
	** If a connection URL to a replica set is given (a connection URL with multiple hosts) then 
	** the hosts are queried to find the primary. The primary is currently used for all read and 
	** write operations. 
	abstract This startup()

	** (Advanced)
	** Authenticates the given connection against the Master, with credentials given via the Mongo connection URL.
	@NoDoc	// advanced
	abstract Void authenticateConn(MongoConn conn)
	
	** Makes a connection available to the given function.
	** 
	** What ever is returned from the func is returned from the method.
	** 
	** Any 'IOErrs' thrown in the fn are assumed to be networking errors, and invoke a topology 
	** recan and a Master failover.
	abstract Obj? leaseConn(|MongoConn->Obj?| c)

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
	abstract Void runInTxn([Str:Obj?]? txnOpts, |Obj| fn)
	
	** (Advanced)
	** To be called on a network 'IOErr'.
	** 
	** Searches the replica set for the Master node - throws 'MongoErr' if the primary can not be found. 
	** 
	** The connection pool is then cleared down and all existing connections closed.
	** All new connections will then re-connect to their new Master.
	@NoDoc	// advanced
	abstract Future failOver() 

	** Closes all MongoDB connections.
	abstract This shutdown()

	** Sets the log level to 'debug' to log all cmd request and responses. 
	virtual This setDebug(Bool debugOn := true) {
		log.level = debugOn ? LogLevel.debug : LogLevel.info
		return this
	}
	
	** Creates a pooled Mongo Connection Manager.
	** 
	** URL examples:
	**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
	**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
	** 
	** If user credentials are supplied, they are used as default authentication for each connection. 
	** 
	** See `https://www.mongodb.com/docs/manual/reference/connection-string/`.
	static new make(Uri connectionUrl, Log? log := null, ActorPool? actorPool := null) { 
		MongoConnMgrPool(connectionUrl, log)
	}
}
