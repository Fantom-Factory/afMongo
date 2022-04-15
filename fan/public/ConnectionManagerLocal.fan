using afConcurrent::LocalRef

@NoDoc	// Should be internal, but it might prove useful.
const class ConnectionManagerLocal : ConnectionManager {
	private  const Unsafe		connectionRef
	override const Uri?			mongoUrl	
	override const MongoCreds?	mongoCreds
	override const Str:Obj?		writeConcern	:= Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("j", false)
	
	new make(Connection connection, Uri mongoUrl) {
		this.mongoUrl		= mongoUrl
		this.connectionRef	= Unsafe(connection)
	}
	
	override Obj? leaseConnection(|Connection->Obj?| c) {
		if (connectionRef.val == null)
			throw MongoErr("No connection is available in this thread!?")
		return c(connectionRef.val)
	}
	
	override This startup() {
		return this
	}

	override This shutdown() {
		(connectionRef.val as Connection)?.close
		return this
	}
}
