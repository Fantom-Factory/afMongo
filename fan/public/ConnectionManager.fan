using concurrent
using inet

const mixin ConnectionManager {
	abstract Connection getConnection()
	
	abstract Void shutdown()
	
	Operation operation() {
		Operation(getConnection)
	}
}

@NoDoc
const class ConnectionManagerSingleThread : ConnectionManager {
	
	new make(Connection connection) {
		Actor.locals["afMongo.connection"] = connection
	}
	
	override Connection getConnection() {
		return Actor.locals["afMongo.connection"]		
	}
	
	override Void shutdown() {
		getConnection.close
	}
}
