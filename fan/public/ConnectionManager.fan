using afConcurrent
using inet

** Manages connections to a MongoDB instance.
** 
** @see `ConnectionManagerPooled`
const mixin ConnectionManager {
	
	** Basic details of where this 'ConnectionManager' connects to, for debugging purposes.
	** When connecting to replica sets, this should indicate the primary. 
	** 
	** It *should not* contain any user credentials and *should* be safe to log. 
	abstract Uri? mongoUrl()
	
	** The default write concern that all write operations use if none supplied.
	** 
	** Defaults to '["w": 1, "wtimeout": 0, "journal": false]'
	**  - write operations are acknowledged,
	**  - write operations never time out,
	**  - write operations need not be committed to the journal.
	abstract Str:Obj? writeConcern()
	
	** Makes a connection available to the given function.
	** 
	** What ever is returned from the func is returned from the method.
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


