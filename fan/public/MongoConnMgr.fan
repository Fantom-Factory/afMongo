using concurrent::ActorPool

** Manages connections to a MongoDB instance.
** 
const mixin MongoConnMgr {
	
	** The log instance used to report warnings.
	abstract Log log()
	
	** Basic details of where this 'ConnectionManager' connects to, for debugging purposes.
	** When connecting to replica sets, this should indicate the primary. 
	** 
	** It *should not* contain any user credentials and *should* be safe to log. 
	abstract Uri? mongoUrl()
	
	** The default write concern that all write operations should use.
	abstract [Str:Obj?]? writeConcern()

	** Makes a connection available to the given function.
	** 
	** What ever is returned from the func is returned from the method.
	** 
	** If the given fn causes an 'IOErr' then the Mongo cluster topology is re-scaaned.
	abstract Obj? leaseConn(|MongoConn->Obj?| c)
	
	** Does what ever the 'ConnectionManager' needs to do to initialise itself.
	** 
	** Often this would be create database connections or other network related activity that it 
	** may not wish to do inside a ctor.
	abstract This startup()
	
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
	**  - 'w'
	**  - 'wtimeoutMS'
	**  - 'journal'
	**  - 'ssl'
	**  - 'tls'
	**  - 'authSource'
	**  - 'authMechanism'
	**  - 'authMechanismProperties'
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


