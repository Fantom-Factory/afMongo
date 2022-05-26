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
** The following URL options are supported:
**  - 'minPoolSize'
**  - 'maxPoolSize'
**  - 'waitQueueTimeoutMS'
**  - 'connectTimeoutMS'
**  - 'socketTimeoutMS'
**  - 'maxIdleTimeMS'
**  - 'w'
**  - 'wtimeoutMS'
**  - 'journal'
**  - 'ssl'
**  - 'tls'
**  - 'authSource'
**  - 'authMechanism'
**  - 'authMechanismProperties'
**  - 'appname'
**  - 'compressors'
**  - 'zlibCompressionLevel'
**  - 'retryWrites'
**  - 'retryReads'
** 
** URL examples:
**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
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
	abstract Uri? mongoUrl()
	
	** The default database name, taken from the the Connection URL auth source.
	abstract Str? dbName()
	
	** The default write concern that all write operations should use.
	abstract [Str:Obj?]? writeConcern()
	
	** Returns 'true' if retryable reads are enabled (the default).
	** Use the connection URL query '?retryReads=false' to disable. 
	abstract Bool retryReads()	

	** Returns 'true' if retryable writes are enabled (the default).
	** Use the connection URL query '?retryWrites=false' to disable. 
	abstract Bool retryWrites()
	
	** Returns 'true' if the server type is standalone and does not declare any hosts.
	** Required info for transactions and retryable writes.
	abstract Bool isStandalone()

	** Does what ever the 'ConnectionManager' needs to do to initialise itself.
	** 
	** Often this would be create database connections or other network related activity that it 
	** may not wish to do inside a ctor.
	abstract This startup()

	** (Advanced)
	** To be called on a network 'IOErr'.
	** 
	** Searches the replica set for the Master node - throws 'MongoErr' if the primary can not be found. 
	** 
	** The connection pool is then cleared down and all existing connections closed.
	** All new connections will then re-connect to their new Master.
	@NoDoc	// advanced
	abstract Future failOver() 

	** (Advanced)
	** Authenticates the given connection against the Master.
	@NoDoc	// advanced
	abstract Void authenticateConn(MongoConn conn)
	
	** Makes a connection available to the given function.
	** 
	** What ever is returned from the func is returned from the method.
	** 
	** Any 'IOErrs' thrown in the fn are assumed to be networking errors, and invoke a topology 
	** recan and a Master failover.
	abstract Obj? leaseConn(|MongoConn->Obj?| c)

	** Closes all MongoDB connections.
	abstract This shutdown()

	** Creates a pooled Mongo Connection Manager.
	** 
	** The following connection URL options are supported:
	**  - 'minPoolSize'
	**  - 'maxPoolSize'
	**  - 'waitQueueTimeoutMS'
	**  - 'connectTimeoutMS'
	**  - 'socketTimeoutMS'
	**  - 'maxIdleTimeMS'
	**  - 'w'
	**  - 'wtimeoutMS'
	**  - 'journal'
	**  - 'ssl'
	**  - 'tls'
	**  - 'authSource'
	**  - 'authMechanism'
	**  - 'authMechanismProperties'
	**  - 'appname'
	**  - 'compressors'
	**  - 'zlibCompressionLevel'
	**  - 'retryWrites'
	**  - 'retryReads'
	** 
	** URL examples:
	**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
	**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
	** 
	** See `https://www.mongodb.com/docs/manual/reference/connection-string/`.
	static new make(Uri connectionUrl, Log? log := null) { 
		MongoConnMgrPool(connectionUrl, log)
	}
}
