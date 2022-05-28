using concurrent::Future
using concurrent::AtomicBool
using concurrent::AtomicInt
using concurrent::AtomicRef
using concurrent::Actor
using concurrent::ActorPool

internal const class MongoConnMgrStub : MongoConnMgrPool {
	
	private const AtomicBool	isStandaloneRef		:= AtomicBool(false)
	private const AtomicBool	retryReadsRef		:= AtomicBool(true)
	private const AtomicBool	retryWritesRef		:= AtomicBool(true)
	private const AtomicRef		writeConcernRef		:= AtomicRef(null)
	private const AtomicInt		failoverCountRef	:= AtomicInt(0)
	private const Unsafe		connRef

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

	new make(MongoConnStub conn) : super(`mongodb://foo.com/bar`, null, null) {
		connRef = Unsafe(conn)
		conn._sessPool = this->sessPool
		startup
	}
	
	This debugOn() {
		log.level = LogLevel.debug
		return this
	}
	
	Int failoverCount() {
		failoverCountRef.getAndSet(0)
	}
	
	override Void huntThePrimary() {
		this->connectionState->connFactory = |->MongoConn| { this.connRef.val } 
	}

	override Future failOver() {
		failoverCountRef.increment
//		Err("FAIL!").trace
		return Future.makeCompletable.complete(69)
	}

	override MongoConn newMongoConn() {
		Actor.locals["afMongo.connStub"]
	}

	override Void authenticateConn(MongoConn conn) { }
	
	MongoSess? sess() {
		// there should only ever be one
		this->sessPool->sessions->first
	}
	
	Int nextTxnNum() {
		1.plus(this->sessPool->transactionNumRef->val)
	}
	
}
