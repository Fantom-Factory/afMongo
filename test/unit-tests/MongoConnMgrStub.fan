using concurrent::Future

internal const class MongoConnMgrStub : MongoConnMgr {
	
	This debugOn() {
		log.level = LogLevel.debug
		return this
	}
	
	override Log log() {
		typeof.pod.log
	}
	
	override Uri? mongoUrl() {
		`mongodb://example.com/wotever`
	}
	
	override Bool tls() {
		false
	}
	
	override Str? database() {
		"wotever"
	}
	
	override [Str:Obj?]? writeConcern() { null }
	
	override Bool retryReads()	{ true }

	override Bool retryWrites()	{ true }
	
	override Bool isStandalone() { false }

	override This startup()	{ this }

	override Future failOver() { Future.makeCompletable.complete(69) }

	override Void authenticateConn(MongoConn conn) { }
	
	override Obj? leaseConn(|MongoConn->Obj?| c) {
		return null
	}

	override This shutdown() { this }
	
}
