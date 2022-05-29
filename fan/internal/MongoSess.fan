using afBson::Binary
using afBson::BsonIO
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
			const AtomicRef		_txnNumRef

	Bool isDetached {
		get { _isDetachedRef.val }
		set { _isDetachedRef.val = it }
	}
	
	Int? txnNum {
		get { _txnNumRef.val }
		set { _txnNumRef.val = it }
	}

	new make(MongoSessPool sessPool) {
		this._sessPool		= sessPool
		this._sessionId		= generateSessionUuid
		this._lastUseRef	= AtomicRef(Duration.now)
		this._isDirtyRef	= AtomicBool(false)
		this._isDetachedRef	= AtomicBool(false)
		this._txnNumRef		= AtomicRef(null)
	}
	
	Bool isInTxn() {
		txnNum != null
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
	
	Void runInTxn(MongoConnMgr connMgr, [Str:Obj?]? txnOpts, |MongoTxn| fn) {
		
		txn := null as MongoTxn
		doRunInTxn := |->| {
			txn    = MongoTxn(connMgr, this, _sessPool.newTxNum)
			txnNum = txn.txnNum
			try		txn.run(txnOpts, fn)
			finally	txnNum = null
		}
		
		// like the rest of the Mongo err handling strategy, let's only re-try the once
		// else if the network plug is pulled, we get stuck in an infinite loop!
		try 	doRunInTxn()
		catch	(IOErr ioe) {
			
			// we do NOT rerun funcs on commit or abort errors
			if (txn.funcRun)
				throw ioe
			
			// all network errors are to be treated like TransientTransactionError
			// use get(), so any failover errors are thrown 
			connMgr.failOver.get(10sec)	// the spec gives an example of 10sec	
			doRunInTxn()
			
		} catch (MongoErr me) {
			// we do NOT rerun funcs on commit or abort errors
			if (txn.funcRun)
				throw me

			if (me.errLabels.contains("TransientTransactionError"))
				doRunInTxn()
			// KEEP this else, we're NOT returning fn(this)
			else throw me
		}
	}
	
	Void prepCmdForTxn(Str:Obj? cmd) {
		txn(true).prepCmd(cmd)
	}

	Void postCmd(Str:Obj? res) {
		txn(false)?.postCmd(res)
	}
	
	** Called after a txn
	Void postTxnCheckin() {
		txnNum = null
		_sessPool.checkin(this, true)
	}
	
	private MongoTxn? txn(Bool checked) {
		txn := MongoTxn.cur
		if (txn == null && checked == false)
			return null
		// this a little leap of faith - so just double check that we ARE associated with the current txn
		if (isInTxn == false || txnNum != txn?.txnNum || this !== txn?.sess)
			throw Err("MongoSess is NOT part of current Txn!? (txnNum ${txnNum} != ${txn?.txnNum})")
		return txn
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
