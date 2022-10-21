using concurrent::AtomicRef
using concurrent::AtomicInt
using concurrent::ActorPool
using afBson::Timestamp
using afConcurrent::SynchronizedList

** Sessions are painful overhead, but a necessary feature that underpins retryable writes and transactions.
** 
** Sessions are stored on a FILO stack (as per Mongo spec) - (I guess) so fewer sessions are active at any one time.
** 
** Session stack, clusterTime, and txNums are all separate features, but given they're all related it made sense to 
** bung them together in the one class. 
** 
** https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst
internal const class MongoSessPool {
	
	const private AtomicRef			sessionTimeoutRef
	const private AtomicRef			clusterTimeRef
	const private AtomicInt			transactionNumRef
	const private SynchronizedList?	sessionPool

	[Str:Obj?]? clusterTime {
		get { clusterTimeRef.val }
		set { clusterTimeRef.val = it.toImmutable }
	}

	Duration sessionTimeout {
		get { sessionTimeoutRef.val }
		set { sessionTimeoutRef.val = it }
	}
	
	// for testing
	MongoSess[] sessions {
		get { sessionPool.val }
		set { sessionPool.val = it }
	}

	new make(ActorPool actorPool) {
		this.sessionTimeoutRef	= AtomicRef(10min)	// don't let conns get stale during testing!
		this.clusterTimeRef		= AtomicRef(null)
		this.transactionNumRef	= AtomicInt(0)
		this.sessionPool		= SynchronizedList(actorPool)
	}
	
	MongoSess checkout() {
		// discard any stored stale sessions (from the top of the stack)
		// this algorithm avoids concurrent race conditions
		sess := null as MongoSess
		while ((sess = sessionPool.pop) != null && sess.isStale)
			null?.toStr
		
		if (sess == null)
			sess = MongoSess(this)

		return sess
	}
	
	Void checkin(MongoSess? sess, Bool force := false) {
		// discard any stored stale sessions (from the bottom of the stack)
		// MongoDB specs say we don't need to check the entire stack
		// this algorithm avoids concurrent race conditions
		stale := null as MongoSess
		while ((stale = sessionPool.first) != null && stale.isStale)
			sessionPool.remove(stale)

		if (sess == null)	return
		if (sess.isDirty)	return
		if (sess.isStale)	return
		if (sess.isInTxn)	return	// it'll be checked in after the transaction
		if (sess.isDetached && force == false)
							return
	
		// "force" checkins happen when a cursor is killed and it wishes to return its sess to the pool 
		sess.isDetached = false
		sessionPool.push(sess)
	}

	** Uses the given Conn to end all sessions and empty the pool.
	Void shutdown(MongoConn conn) {
		sessIds := sessionPool.val.map |MongoSess sess->Obj?| { sess.sessionId }
		cmd		:= map.add("endSessions", sessIds)
		
		// ignore the response, I don't care if it failed - this op call is just a courtesy
		MongoOp(null, conn, cmd).runCommand("admin", false)
		
		sessionPool.clear
	}
	
	Void updateClusterTime([Str:Obj?]? serverTime) {
		// I'm not concerned about concurrent race conditions here,
		// as we're only gossiping an approx timestamp
		if (serverTime == null)
			return
		
		if (clusterTime == null) {
			clusterTime = serverTime
			return
		}

		serverTs := serverTime["clusterTime"] as Timestamp
		if (serverTs == null)
			return
		
		if (serverTs > clusterTs)
			clusterTime = serverTime
	}
	
	Void appendClusterTime(Str:Obj? cmd) {
		if (clusterTime != null)
			cmd["\$clusterTime"] = clusterTime
	}
	
	Int newTxNum() {
		transactionNumRef.incrementAndGet
	}
	
	private Timestamp? clusterTs() {
		clusterTime?.get("clusterTime")
	}

	private [Str:Obj?] map() { 
		map := Str:Obj?[:]
		map.ordered = true 
		return map
	}
}
