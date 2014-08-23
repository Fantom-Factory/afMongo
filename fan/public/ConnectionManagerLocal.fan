using afConcurrent

@NoDoc	// Should be internal, but it might prove useful.
const class ConnectionManagerLocal : ConnectionManager {
	private const LocalRef connectionRef	:= LocalRef("afMongo.connection")
	
	override const Uri mongoUrl	

	override Uri mongoUri() {
		mongoUrl
	}
	
	new make(Connection connection, Uri mongoUrl) {
		this.mongoUrl = mongoUrl
		this.connectionRef.val = connection
	}
	
	override Obj? leaseConnection(|Connection->Obj?| c) {
		// TODO: throw Err if connection doesn't exist in this thread
		c(connectionRef.val)
	}
	
	override ConnectionManager startup() {
		return this
	}

	override ConnectionManager shutdown() {
		(connectionRef.val as Connection)?.close
		connectionRef.cleanUp
		return this
	}
}
