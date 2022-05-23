using afBson::Binary
using afBson::BsonIO
using concurrent::Actor
using concurrent::AtomicRef
using concurrent::AtomicBool

** A Mongo Logical Session.
**  
** And yes, this IS named as such just so I can have a MongoSessPool!!!
@NoDoc
internal const class MongoSess {

	private	const MongoSessPool	_sessPool
	private const [Str:Obj?]	_sessionId
	private const AtomicRef		_lastUseRef
	private const AtomicBool	_isDirtyRef
			const AtomicBool	_isDetachedRef

	Bool isDetached {
		get { _isDetachedRef.val }
		set { _isDetachedRef.val = it }
	}
	
	new make(MongoSessPool sessPool) {
		this._sessPool		= sessPool
		this._sessionId		= generateSessionUuid
		this._lastUseRef	= AtomicRef(Duration.now)
		this._isDirtyRef	= AtomicBool(false)
		this._isDetachedRef	= AtomicBool(false)
	}

	Uuid uuid() {
		bin := _sessionId["id"] as Binary
		buf := bin.data.dup
		return Uuid.makeBits(buf.readS8, buf.readS8)
	}

	** Returns the sessionId and updates the lastUse time.
	Str:Obj? sessionId() {
		this._lastUseRef.val = Duration.now
		return _sessionId
	}

	Void advanceClusterTime(Str:Obj? clusterTime) {
		throw UnsupportedErr()
	}

	** Returns the session to the pool to be reused.
	Void endSession() {
		_sessPool.checkin(this, true)
	}

	** Called when there's a network error to ensure the session is not returned to the pool.
	** However, it may *still* be used for op retries and other session related purposes.
	Void markDirty() {
		_isDirtyRef.val = true
	}

	Bool isDirty() {
		_isDirtyRef.val
	}

	Bool isStale() {
		timeToLive := lastUse + _sessPool.sessionTimeout - Duration.now
		return timeToLive < 1min
	}
	
	Void updateClusterTime([Str:Obj?]? serverTime) {
		_sessPool.updateClusterTime(serverTime)
	}
	
	Void appendClusterTime(Str:Obj? cmd) {
		_sessPool.appendClusterTime(cmd)
	}
	
	Int newTxNum() {
		_sessPool.newTxNum
	}
	
	private Duration lastUse() {
		_lastUseRef.val
	}

	private Str:Obj? generateSessionUuid() {
		uuid := Uuid()
		buf	 := Buf().writeI8(uuid.bitsHi).writeI8(uuid.bitsLo).flip
		return Str:Obj?["id" : Binary(buf, Binary.BIN_UUID)]
	}
	
	@NoDoc
	override Str toStr() {
		str := ""
		str += "ID:" + uuid.toStr[9..22]	// the other digits don't change, so don't bother printing them
		ttl := lastUse + _sessPool.sessionTimeout - Duration.now
		str += " TTL:${ttl.toLocale}"
		if (_isDirtyRef.val)
		str += " (dirty)"
		if (_isDetachedRef.val)
		str += " (detached)"
		return str
	}
}
