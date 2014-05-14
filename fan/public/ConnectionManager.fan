using afConcurrent
using inet

** Manages connections to a MongoDB instance.
const mixin ConnectionManager {
	
	** Makes a connection available to the given function.
	abstract Obj? leaseConnection(|Connection->Obj?| c)
	
	** Closes all MongoDB connections.
	abstract Void shutdown()
}

const class ConnectionManagerSingleThread : ConnectionManager {
	private const LocalRef connectionRef	:= LocalRef("afMongo.connection")
	
	new make(Connection connection) {
		connectionRef.val = connection
	}
	
	override Obj? leaseConnection(|Connection->Obj?| c) {
		// TODO: throw Err if connection doesn't exist in this thread
		c(connectionRef.val)
	}
	
	override Void shutdown() {
		(connectionRef.val as Connection)?.close
		connectionRef.cleanUp
	}
}

