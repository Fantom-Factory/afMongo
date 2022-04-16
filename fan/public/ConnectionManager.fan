using concurrent::ActorPool

** Manages connections to a MongoDB instance.
** 
** @see `ConnectionManagerPooled`
const mixin ConnectionManager {
	
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

	
	// FIXME makePool()
	
}


