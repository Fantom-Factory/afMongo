using afConcurrent::LocalRef

@NoDoc	// Should be internal, but it might prove useful.
const class ConnectionManagerLocal : ConnectionManager {
	private  const Unsafe		connectionRef
	override const Log			log
	override const Uri?			mongoUrl	
	override const [Str:Obj?]?	writeConcern
	
	new make(Connection connection, Uri mongoUrl) {
		this.mongoUrl		= mongoUrl
		this.connectionRef	= Unsafe(connection)
		this.log			= ConnectionManagerLocal#.pod.log
	}
	
	override Obj? leaseConnection(|Connection->Obj?| c) {
		c(connectionRef.val)
	}
	
	override This startup() {
		return this
	}

	override This shutdown() {
		(connectionRef.val as Connection)?.close
		return this
	}
}
