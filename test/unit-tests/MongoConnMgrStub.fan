using concurrent::Future
using concurrent::AtomicBool
using concurrent::AtomicInt
using concurrent::AtomicRef
using concurrent::Actor
using concurrent::ActorPool

internal const class MongoConnMgrStub : MongoConnMgrPool {
	
	private const AtomicBool	isStandaloneRef		:= AtomicBool(false)
	private const AtomicInt		failoverCountRef	:= AtomicInt(0)
	private const Unsafe		connRef

	override Bool isStandalone {
		get { isStandaloneRef.val }
		set { isStandaloneRef.val = it }
	}

	new make(MongoConnStub conn, Uri? url := null) : super(url ?: `mongodb://foo.com/bar`, null, null) {
		connRef = Unsafe(conn)
		conn._sessPool = this->sessPool
		startup
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
