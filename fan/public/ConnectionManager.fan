using afConcurrent
using inet

** Manages connections to a MongoDB instance.
** 
** @see `ConnectionManagerPooled`
const mixin ConnectionManager {
	
	** Makes a connection available to the given function.
	abstract Obj? leaseConnection(|Connection->Obj?| c)
	
	** Closes all MongoDB connections.
	abstract Void shutdown()
}


