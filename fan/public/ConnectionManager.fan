using afConcurrent
using inet

const mixin ConnectionManager {
	
	abstract Obj? leaseConnection(|Connection->Obj?| c)
	
	abstract Void shutdown()

}

@NoDoc
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

