using concurrent::Future
using concurrent::AtomicBool
using concurrent::AtomicInt
using concurrent::AtomicRef
using concurrent::Actor

internal const class MongoConnMgrStub : MongoConnMgr {
	
	private const AtomicBool	isStandaloneRef		:= AtomicBool(false)
	private const AtomicBool	retryReadsRef		:= AtomicBool(true)
	private const AtomicBool	retryWritesRef		:= AtomicBool(true)
	private const AtomicRef		writeConcernRef		:= AtomicRef(null)
	private const AtomicInt		failoverCountRef	:= AtomicInt(0)

	override Bool isStandalone {
		get { isStandaloneRef.val }
		set { isStandaloneRef.val = it }
	}
	
	override Bool retryReads {
		get { retryReadsRef.val }
		set { retryReadsRef.val = it }
	}
	
	override Bool retryWrites {
		get { retryWritesRef.val }
		set { retryWritesRef.val = it }
	}
	
	override [Str:Obj]? writeConcern {
		get { writeConcernRef.val }
		set { writeConcernRef.val = it?.toImmutable }
	}

	new make(MongoConnStub? conn := null) {
		Actor.locals["afMongo.connStub"] = conn
	}
	
	This debugOn() {
		log.level = LogLevel.debug
		return this
	}
	
	Int failoverCount() {
		failoverCountRef.getAndSet(0)
	}
	
	override Log log() {
		typeof.pod.log
	}
	
	override Uri? mongoUrl() {
		`mongodb://example.com/wotever`
	}
	
	override Str? database() {
		"wotever"
	}
	
	override This startup()	{ this }

	override Future failOver() {
		failoverCountRef.increment
		return Future.makeCompletable.complete(69)
	}

	override Void authenticateConn(MongoConn conn) { }
	
	override Obj? leaseConn(|MongoConn->Obj?| c) {
		conn := Actor.locals["afMongo.connStub"]
		return c(conn)
	}

	override This shutdown() { this }
	
}
