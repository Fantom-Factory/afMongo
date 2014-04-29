using inet

const class MongoClient {
	
	private const ConnectionManager conMgr
	
	** A convenience ctor
	new makeWithTcpDetails(Str address := "127.0.0.1", Int port := 27017) {
		this.conMgr = ConnectionManagerSingleThread(TcpConnection(IpAddr(address), port))
	}
	
	new makeWithConnectionManager(ConnectionManager connectionManager) {
		this.conMgr = connectionManager
	}
	
	Database db(Str dbName) {
		Database(conMgr, dbName)
	}

	@Operator
	Database get(Str dbName) {
		db(dbName)
	}
	
	Str[] databaseNames() {
		databases	:= (Map[]) runAdminCommand(["listDatabases":1])["databases"]
		names 		:= databases.map { it["name"].toStr }.sort
		return names
	}

	// TODO: add params to getlast err
	** @see `http://docs.mongodb.org/manual/reference/command/getLastError/`
	Str:Obj? getLastError() {
		runAdminCommand(["getLastError":1])
	}
	
	Str:Obj? runAdminCommand(Str:Obj? cmd) {
		conMgr.operation.runCommand("admin.\$cmd", cmd)
	}

	** Convenience for 'connectionManager.shutdown'.
	Void shutdown() {
		conMgr.shutdown
	}
}
