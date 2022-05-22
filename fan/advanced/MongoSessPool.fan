using concurrent::AtomicRef
using afBson::Timestamp

** Sessions are painful overhead, but a necessary feature that underpins retryable writes and transactions.
** 
** https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst
internal const class MongoSessPool {
	
	const private AtomicRef	sessionTimeoutRef
	const private AtomicRef	clusterTimeRef
	
	MongoSess[] sessPool() {
		MongoSess[,]
	}

	[Str:Obj?]? clusterTime {
		get { clusterTimeRef.val }
		set { clusterTimeRef.val = it.toImmutable }
	}

	Duration sessionTimeout {
		get { sessionTimeoutRef.val }
		set { sessionTimeoutRef.val = it }
	}

	new make() {
		this.sessionTimeoutRef	= AtomicRef(null)
		this.clusterTimeRef		= AtomicRef(null)
	}
	
	MongoSess checkout() {
		// this algo avoids concurrent race conditions
		sess := null as MongoSess
		while ((sess = sessPool.pop) != null && sess.isStale)
			null?.toStr
		
		if (sess == null)
			sess = MongoSess(this)
		
		return sess
	}
	
	Void checkin(MongoSess? sess, Bool force := false) {
		// MongoDB specs say we don't need to check the entire stack
		// this algo avoids concurrent race conditions
		stale := null as MongoSess
		while ((stale = sessPool.first) != null && stale.isStale)
			sessPool.removeSame(stale)
		
		if (sess == null)	return
		if (sess.isDirty)	return
		if (sess.isStale)	return
		if (sess.isDetached && force == false)
							return
		
		sessPool.push(sess)
	}

	** Uses the given Conn to end all sessions and empty the pool.
	Void shutdown(MongoConn conn) {
		sessIds := sessPool.map |MongoSess sess->Obj?| { sess.sessionId }
		cmd		:= map.add("endSessions", sessIds)
		
		// ignore the response, I don't care if it failed - this op call is just a courtesy
		MongoOp(conn, cmd).runCommand("admin", false)
		
		sessPool.clear
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
