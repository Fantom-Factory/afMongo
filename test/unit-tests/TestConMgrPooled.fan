using concurrent

internal class TestConMgrPooled : MongoTest {
	
	Void testMongoUriUserCreds() {
		pool := ActorPool()
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badScheme(`dude://wotsup?`)) {
			conMgr := ConnectionManagerPooled(pool, `dude://wotsup?`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badUsernamePasswordCombo("user", "null", `mongodb://user@wotever`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://user@wotever`)
		}

		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badUsernamePasswordCombo("null", "pass", `mongodb://:pass@wotever`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://:pass@wotever`)
		}
		
		conMgr := ConnectionManagerPooled(pool, `mongodb://user:pass@wotever/puppies`)
		verifyEq(conMgr.defaultDatabase, "puppies")
		verifyEq(conMgr.defaultUsername, "user")
		verifyEq(conMgr.defaultPassword, "pass")

		conMgr = ConnectionManagerPooled(pool, `mongodb://user:pass@wotever`)
		verifyEq(conMgr.defaultDatabase, "admin")

		conMgr = ConnectionManagerPooled(pool, `mongodb://user:pass@wotever/`)
		verifyEq(conMgr.defaultDatabase, "admin")

		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever/puppies`)
		verifyEq(conMgr.defaultDatabase, null)
	}
		
	Void testMongoUriPoolSize() {
		pool := ActorPool()

		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMinConnectionSize(-1, `mongodb://wotever?minPoolSize=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=-1`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMaxConnectionSize(0, `mongodb://wotever?maxPoolSize=0`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?maxPoolSize=0`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMinMaxConnectionSize(2, 1, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)
		}
		
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=15`)
		verifyEq(conMgr.minPoolSize, 2)
		verifyEq(conMgr.maxPoolSize, 15)
	}
}
