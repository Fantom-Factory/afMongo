using afConcurrent
using inet

** Manages connections to a MongoDB instance.
** 
** @see `ConnectionManagerPooled`
const mixin ConnectionManager {
	
	** Makes a connection available to the given function.
	abstract Obj? leaseConnection(|Connection->Obj?| c)
	
	** Does what ever the 'ConnectionManager' needs to do to initialise itself.
	** 
	** Often this would be create database connections or other network related activity that it 
	** may not wish to do inside a ctor.
	abstract This startup()
	
	** Closes all MongoDB connections.
	abstract This shutdown()
}


