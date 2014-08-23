using afConcurrent
using inet

** Manages connections to a MongoDB instance.
** 
** @see `ConnectionManagerPooled`
const mixin ConnectionManager {
	
	** Basic details of where this 'ConnectionManager' connects to, for debugging purposes.
	** It *should not* contain any user credentials and *should* be safe to log. 
	abstract Uri mongoUrl()
	
	@NoDoc @Deprecated { msg="Use mongoUrl() instead" }
	abstract Uri mongoUri()
	
	** Makes a connection available to the given function.
	abstract Obj? leaseConnection(|Connection->Obj?| c)
	
	// Can't return 'This' because of Plastic Proxies.
	** Does what ever the 'ConnectionManager' needs to do to initialise itself.
	** 
	** Often this would be create database connections or other network related activity that it 
	** may not wish to do inside a ctor.
	abstract ConnectionManager startup()
	
	// Can't return 'This' because of Plastic Proxies.
	** Closes all MongoDB connections.
	abstract ConnectionManager shutdown()
}


