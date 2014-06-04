using concurrent

internal class TestConMgrPooled : MongoTest {
	
	Void testMongoUri() {
		pool := ActorPool()
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badScheme(`dude://wotsup?`)) {
			conMgr := ConnectionManagerPooled(pool, `dude://wotsup?`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badUsernamePasswordCombo("user", "", `mongodb://user@wotever`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://user@wotever`)
		}

		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badUsernamePasswordCombo("", "pass", `mongodb://:pass@wotever`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://:pass@wotever`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMinConnectionSize(-1, `mongodb://wotever?minPoolSize=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=-1`).startup
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMaxConnectionSize(0, `mongodb://wotever?maxPoolSize=0`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?maxPoolSize=0`).startup
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMinMaxConnectionSize(2, 1, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`).startup
		}
	}
}
