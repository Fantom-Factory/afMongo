using afBson::Binary
using afBson::BsonIO

** A Mongo Logical Session.
**  
** And yes, this IS named as such just so I can have a MongoSessPool!!!
@NoDoc
internal class MongoSess {
	
	private	MongoSessPool	_sessPool
	private [Str:Obj?]?		_clusterTime	// Str:Obj?
	private [Str:Obj?]		_sessionId
	private Duration		_lastUse
	private Bool			_isDirty
			Bool			isDetached

	new make(MongoSessPool sessPool) {
		this._sessPool	= sessPool
		this._sessionId	= generateSessionUuid
		this._lastUse	= Duration.now
		this._isDirty	= false
	}

	** Returns the sessionId and updates the lastUse time.
	Str:Obj? sessionId() {
		this._lastUse	= Duration.now
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
		_isDirty = true
	}

	Bool isDirty() {
		_isDirty
	}

	Bool isStale() {
		timeToLive := _lastUse + _sessPool.sessionTimeout - Duration.now
		return timeToLive < 1min
	}

	private Str:Obj? generateSessionUuid() {
		uuid := Uuid()
		buf	 := Buf().writeI8(uuid.bitsHi).writeI8(uuid.bitsLo).flip
		return Str:Obj?["id" : Binary(buf, Binary.BIN_UUID)]
	}
	
	@NoDoc
	override Str toStr() {
		BsonIO().print(_sessionId)
	}
}
