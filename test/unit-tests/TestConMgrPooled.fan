using concurrent

internal class TestConMgrPooled : MongoTest {
	ActorPool pool := ActorPool()

	Void testMongoUriUserCreds() {		
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
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badInt("minPoolSize", "zero", -1, `mongodb://wotever?minPoolSize=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=-1`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badInt("maxPoolSize", "one", 0, `mongodb://wotever?maxPoolSize=0`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?maxPoolSize=0`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badMinMaxConnectionSize(2, 1, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)
		}
		
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=15`)
		verifyEq(conMgr.minPoolSize,  2)
		verifyEq(conMgr.maxPoolSize, 15)
	}

	Void testMongoUriConnectionOptions() {
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badInt("connectTimeoutMS", "zero", -1, `mongodb://wotever?connectTimeoutMS=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?connectTimeoutMS=-1`)
		}
		
		verifyErrMsg(ArgErr#, ErrMsgs.connectionManager_badInt("socketTimeoutMS", "zero", -1, `mongodb://wotever?socketTimeoutMS=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?socketTimeoutMS=-1`)
		}
		
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?connectTimeoutMS=2000&socketTimeoutMS=3000`)
		verifyEq(conMgr.connectTimeout, 2sec)
		verifyEq(conMgr.socketTimeout,  3sec)
		
	}
}
