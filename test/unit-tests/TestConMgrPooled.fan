using concurrent::ActorPool
using afConcurrent::LocalRef

internal class TestConMgrPooled : MongoTest {
	ActorPool pool := ActorPool()

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
}
