using concurrent::ActorPool
using afConcurrent::LocalRef

internal class TestConMgrPooled : MongoTest {
	ActorPool pool := ActorPool()

	private static LogRec[] logs() { ulogs.val }
	private static const Unsafe ulogs := Unsafe(LogRec[,])
	private static const |LogRec rec| handler := |LogRec rec| { logs.add(rec) }
	
	override Void setup() {
		logs.clear
		Log.addHandler(handler)
	}
	
	override Void teardown() {
		Log.removeHandler(handler)		
	}
	
	Void testMongoUriUserCreds() {		
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badScheme(`dude://wotsup?`)) {
			conMgr := ConnectionManagerPooled(pool, `dude://wotsup?`)
		}
		
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badUsernamePasswordCombo("user", "null", `mongodb://user@wotever`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://user@wotever`)
		}

		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badUsernamePasswordCombo("null", "pass", `mongodb://:pass@wotever`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://:pass@wotever`)
		}

		conMgr := ConnectionManagerPooled(pool, `mongodb://user:pass@wotever/puppies`)
		verifyEq(conMgr.mongoCreds.source, "puppies")
		verifyEq(conMgr.mongoCreds.username, "user")
		verifyEq(conMgr.mongoCreds.password, "pass")

		verify(logs.isEmpty)
	}	

	Void testMongoUriAuthMech() {		
		conMgr := ConnectionManagerPooled(pool, `mongodb://user:pass@wotever/puppies?authSource=kinesis&authMechanism=pacificRim&authMechanismProperties=cannon:Plasmacaster,,,turbine:NuclearVortex`)
		verifyEq(conMgr.mongoCreds.mechanism,	"pacificRim")
		verifyEq(conMgr.mongoCreds.source, 		"kinesis")
		verifyEq(conMgr.mongoCreds.username,	"user")
		verifyEq(conMgr.mongoCreds.password,	"pass")
		verifyEq(conMgr.mongoCreds.props,		Str:Obj?[
			"cannon"	: "Plasmacaster",
			"turbine"	: "NuclearVortex",
		])

		verify(logs.isEmpty)
	}
		
	Void testMongoUriPoolOptions() {
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badInt("minPoolSize", "zero", -1, `mongodb://wotever?minPoolSize=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=-1`)
		}
		
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badInt("maxPoolSize", "one", 0, `mongodb://wotever?maxPoolSize=0`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?maxPoolSize=0`)
		}
		
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badMinMaxConnectionSize(2, 1, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=1`)
		}
		
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badInt("waitQueueTimeoutMS", "zero", -1, `mongodb://wotever?waitQueueTimeoutMS=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?waitQueueTimeoutMS=-1`)
		}

		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?minPoolSize=2&maxPoolSize=15&waitQueueTimeoutMS=3000`)
		verifyEq(conMgr.minPoolSize,  2)
		verifyEq(conMgr.maxPoolSize, 15)
		verifyEq(conMgr.waitQueueTimeout, 3sec)
		verify(logs.isEmpty)
	}

	Void testMongoUriConnectionOptions() {
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badInt("connectTimeoutMS", "zero", -1, `mongodb://wotever?connectTimeoutMS=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?connectTimeoutMS=-1`)
		}
		
		verifyErrMsg(ArgErr#, MongoErrMsgs.connectionManager_badInt("socketTimeoutMS", "zero", -1, `mongodb://wotever?socketTimeoutMS=-1`)) {
			conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?socketTimeoutMS=-1`)
		}
		
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?connectTimeoutMS=2000&socketTimeoutMS=3000`)
		verifyEq(conMgr.connectTimeout, 2sec)
		verifyEq(conMgr.socketTimeout,  3sec)
		verify(logs.isEmpty)
	}
	
	Void testWarningsForUnknownQueryOptions() {
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever?dude=wotever&dude2`)
		verifyEq(logs[0].msg, "Unknown option in Mongo connection URL: dude=wotever - mongodb://wotever?dude=wotever&dude2")
		verifyEq(logs[1].msg, "Unknown option in Mongo connection URL: dude2=true - mongodb://wotever?dude=wotever&dude2")
	}

	Void testMongoLabConnectionStr() {
		conMgr	:= ConnectionManagerPooled(pool, `mongodb://user:pass@ds999999-a0.mlab.com:55555,@ds999999-a1.mlab.com:44444/stackhub?replicaSet=rs-ds059296`)
		verifyEq(conMgr.mongoCreds.source,		"stackhub")
		verifyEq(conMgr.mongoCreds.username,	"user")
		verifyEq(conMgr.mongoCreds.password,	"pass")
		verifyEq(conMgr.connectionUrl.host,		"ds999999-a0.mlab.com:55555,@ds999999-a1.mlab.com")
		verifyEq(conMgr.connectionUrl.port,		44444)

		hg := conMgr.connectionUrl.host.split(',')
		hostList := (HostDetails[]) hg.map { HostDetails(it, false) }
		hostList.last.port = conMgr.connectionUrl.port ?: 27017
		verifyEq(hostList[0].address,	"ds999999-a0.mlab.com")
		verifyEq(hostList[0].port, 		55555)
		verifyEq(hostList[1].address,	"ds999999-a1.mlab.com")
		verifyEq(hostList[1].port, 		44444)
	}
	
	Void testBackoffFuncHappyCase() {
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever`) {
			it.sleepFunc  = |Duration d| { }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		result := conMgr.backoffFunc(|->Obj?| { noOfFuncCalls++; return "Done"  }, 10sec)
		
		verifyEq(result, "Done")
		verifyEq(noOfFuncCalls, 1)
	}

	Void testBackoffFuncHappyCasePartial() {
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever`) {
			it.sleepFunc  = |Duration d| { }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		result := conMgr.backoffFunc(|->Obj?| { noOfFuncCalls++; return noOfFuncCalls==3 ? "Done" : null }, 10sec)
		
		verifyEq(result, "Done")
		verifyEq(noOfFuncCalls, 3)
	}

	Void testBackoffFuncUnhappyCase() {
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever`) {
			it.sleepFunc  = |Duration d| { }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		result := conMgr.backoffFunc(|->Obj?| { noOfFuncCalls++; return null }, 10sec)
		
		verifyEq(result, null)
		verifyEq(noOfFuncCalls, 10)	// we just happen to know this! See testBackoffFuncNapTimes below.
	}

	Void testBackoffFuncNapTimes() {
		napTimesU := Unsafe(Duration[,])
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever`) {
			it.sleepFunc  = |Duration d| { napTimesU.val->add(d)  }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		result := conMgr.backoffFunc(|->Obj?| { noOfFuncCalls++; return noOfFuncCalls==10 ? "Done" : null }, 10sec)

		napTimes := (Duration[]) napTimesU.val
		verifyEq(result, "Done")
		verifyEq(napTimes.size, 9)
		verifyEq(napTimes[0], 10ms)
		verifyEq(napTimes[1], 30ms)
		verifyEq(napTimes[2], 70ms)
		verifyEq(napTimes[3], 150ms)
		verifyEq(napTimes[4], 310ms)
		verifyEq(napTimes[5], 630ms)
		verifyEq(napTimes[8], 4980ms)
	}

	Void testBackoffFuncTotalNapTime() {
		napTimeU  := LocalRef("napTime")
		napTime2U := LocalRef("napTime2")
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever`) {
			it.sleepFunc  = |Duration d| { napTimeU.val = napTimeU.val->plus(d)  }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		napTimeU.val = 0sec
		conMgr.backoffFunc(|Duration d->Obj?| { napTime2U.val = d; return null }, 10sec)
		verifyEq(napTimeU.val,  10sec)
		verifyEq(napTime2U.val, 10sec)

		napTimeU.val = 0sec
		conMgr.backoffFunc(|Duration d->Obj?| { napTime2U.val = d; return null }, 3sec)
		verifyEq(napTimeU.val, 3sec)
		verifyEq(napTime2U.val, 3sec)

		napTimeU.val = 0sec
		conMgr.backoffFunc(|Duration d->Obj?| { napTime2U.val = d; return null }, 8.7sec)
		verifyEq(napTimeU.val, 8.7sec)
		verifyEq(napTime2U.val, 8.7sec)
	}
	
	Void testWriteConcent() {
		// test default
		conMgr := ConnectionManagerPooled(pool, `mongodb://wotever`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("j", false))

		// write concern
		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?w=-1`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", -1).add("wtimeout", 0).add("j", false))
		
		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?w=0`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 0).add("wtimeout", 0).add("j", false))
		
		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?w=1`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("j", false))

		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?w=set`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", "set").add("wtimeout", 0).add("j", false))

		// write timeout
		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?wtimeoutMS=2000`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 2000).add("j", false))
		
		// journal
		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?journal=true`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 1).add("wtimeout", 0).add("j", true))
		
		// all options
		conMgr = ConnectionManagerPooled(pool, `mongodb://wotever?w=3&wtimeoutMS=1234&journal=true`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 3).add("wtimeout", 1234).add("j", true))

		verify(logs.isEmpty)
	}
}
