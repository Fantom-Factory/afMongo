using concurrent::AtomicRef
using concurrent::ActorPool
using afBson::Timestamp
using afConcurrent::SynchronizedList

** Sessions are painful overhead, but a necessary feature that underpins retryable writes and transactions.
** 
** https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst
internal const class MongoSessPool {
	
	const private AtomicRef			sessionTimeoutRef
	const private AtomicRef			clusterTimeRef
	const private SynchronizedList?	sessionPool
	
	[Str:Obj?]? clusterTime {
		get { clusterTimeRef.val }
		set { clusterTimeRef.val = it.toImmutable }
	}

	Duration sessionTimeout {
		get { sessionTimeoutRef.val }
		set { sessionTimeoutRef.val = it }
	}

	new make(ActorPool actorPool) {
		this.sessionTimeoutRef	= AtomicRef(null)
		this.clusterTimeRef		= AtomicRef(null)
		this.sessionPool		= SynchronizedList(actorPool)
	}
	
	MongoSess checkout() {
		// this algo avoids concurrent race conditions
		sess := null as MongoSess
		while ((sess = sessionPool.pop) != null && sess.isStale)
			null?.toStr
		
		if (sess == null)
			sess = MongoSess(this)
		
		return sess
	}
	
	Void checkin(MongoSess? sess, Bool force := false) {
		// MongoDB specs say we don't need to check the entire stack
		// this algo avoids concurrent race conditions
		stale := null as MongoSess
		while ((stale = sessionPool.first) != null && stale.isStale)
			sessionPool.remove(stale)

		if (sess == null)	return
		if (sess.isDirty)	return
		if (sess.isStale)	return
		if (sess.isDetached && force == false)
							return
		
		sessionPool.push(sess)
	}

	** Uses the given Conn to end all sessions and empty the pool.
	Void shutdown(MongoConn conn) {
		sessIds := sessionPool.val.map |MongoSess sess->Obj?| { sess.sessionId }
		cmd		:= map.add("endSessions", sessIds)
		
		// ignore the response, I don't care if it failed - this op call is just a courtesy
		MongoOp(conn, cmd).runCommand("admin", false)
		
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
	
	private Timestamp? clusterTs() {
		clusterTime?.get("clusterTime")
	}

	private [Str:Obj?] map() { Str:Obj?[:] { ordered = true } }
}
