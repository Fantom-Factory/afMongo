using concurrent::ActorPool

internal class TestMongoSess : Test {
	
	Void testSessionStack() {
		pool := MongoSessPool(ActorPool())
		pool.sessionTimeout = 10min
		
		ses1 := null as MongoSess
		ses2 := null as MongoSess
		ses3 := null as MongoSess
		
		ses1 = pool.checkout
		verifyEq(pool.sessions.size, 0)
		pool.checkin(ses1)
		verifyEq(pool.sessions.size, 1)
		
		ses1 = pool.checkout
		ses2 = pool.checkout
		verifyEq(pool.sessions.size, 0)
		pool.checkin(ses2)
		verifyEq(pool.sessions.size, 1)
		pool.checkin(ses1)
		verifyEq(pool.sessions.size, 2)
		
		ses1 = pool.checkout
		ses2 = pool.checkout
		ses3 = pool.checkout
		verifyEq(pool.sessions.size, 0)
		pool.checkin(ses3)
		verifyEq(pool.sessions.size, 1)
		pool.checkin(ses2)
		verifyEq(pool.sessions.size, 2)
		pool.checkin(ses1)
		verifyEq(pool.sessions.size, 3)
		
		
		
		ses1 = pool.checkout
		ses2 = pool.checkout
		ses3 = pool.checkout
		verifyEq(pool.sessions.size, 0)
		
		pool.checkin(null)
		verifyEq(pool.sessions.size, 0)
		
		ses3.markDirty
		pool.checkin(ses3)
		verifyEq(pool.sessions.size, 0)
		
		pool.sessionTimeout = 30sec
		verifyEq(ses2.isStale, true)
		pool.checkin(ses2)
		verifyEq(pool.sessions.size, 0)
		pool.sessionTimeout = 10min
		
		ses1.isDetached = true
		pool.checkin(ses1)
		verifyEq(pool.sessions.size, 0)

		pool.checkin(ses1, true)
		verifyEq(pool.sessions.size, 1)
	}
	
}
