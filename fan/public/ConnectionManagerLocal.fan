using afConcurrent

@NoDoc	// Should be internal, but it might prove useful.
const class ConnectionManagerLocal : ConnectionManager {
	private const LocalRef connectionRef	:= LocalRef("afMongo.connection")
	
	override const Uri? mongoUrl	
	override const Str:Obj? writeConcern := Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("j", false)

	new make(Connection connection, Uri mongoUrl, |This|? f := null) {
		f?.call(this)
		this.mongoUrl = mongoUrl
		this.connectionRef.val = connection
	}
	
	override Obj? leaseConnection(|Connection->Obj?| c) {
		if (connectionRef.val == null)
			throw MongoErr(ErrMsgs.connectionManager_noConnectionInThread)
		return c(connectionRef.val)
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
