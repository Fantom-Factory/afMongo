using afConcurrent::LocalRef

internal class TestBackoffFunc : Test {

	Void testBackoffFuncHappyCase() {
		backoff := MongoBackoff {
			it.sleepFunc  = |Duration d| { }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		msgBuf := StrBuf()
		result := backoff.backoffFunc(|->Obj?| { noOfFuncCalls++; return "Done" }, 10sec, msgBuf)
		
		verifyEq(result, "Done")
		verifyEq(noOfFuncCalls, 1)
		verifyEq(msgBuf.toStr, "")
	}

	Void testBackoffFuncHappyCasePartial() {
		backoff := MongoBackoff {
			it.sleepFunc  = |Duration d| { }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		msgBuf := StrBuf()
		result := backoff.backoffFunc(|->Obj?| { noOfFuncCalls++; return noOfFuncCalls==3 ? "Done" : null }, 10sec, msgBuf)
		
		verifyEq(result, "Done")
		verifyEq(noOfFuncCalls, 3)
		verifyEq(msgBuf.toStr,
			"Sleeping for 10ms
			 Sleeping for 30ms")
	}

	Void testBackoffFuncUnhappyCase() {
		backoff := MongoBackoff {
			it.sleepFunc  = |Duration d| { }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		msgBuf := StrBuf()
		result := backoff.backoffFunc(|->Obj?| { noOfFuncCalls++; return null }, 10sec, msgBuf)
		
		verifyEq(result, null)
		verifyEq(noOfFuncCalls, 10)	// we just happen to know this! See testBackoffFuncNapTimes below.
		verifyEq(msgBuf.toStr,
		    "Sleeping for 10ms
		     Sleeping for 30ms
		     Sleeping for 70ms
		     Sleeping for 150ms
		     Sleeping for 310ms
		     Sleeping for 630ms
		     Sleeping for 1270ms
		     Sleeping for 2sec
		     Sleeping for 4sec")
	}

	Void testBackoffFuncNapTimes() {
		napTimesU := Unsafe(Duration[,])
		backoff := MongoBackoff {
			it.sleepFunc  = |Duration d| { napTimesU.val->add(d)  }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		noOfFuncCalls := 0
		msgBuf := StrBuf()
		result := backoff.backoffFunc(|->Obj?| { noOfFuncCalls++; return noOfFuncCalls==10 ? "Done" : null }, 10sec, msgBuf)

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
		verifyEq(msgBuf.toStr,
		    "Sleeping for 10ms
		     Sleeping for 30ms
		     Sleeping for 70ms
		     Sleeping for 150ms
		     Sleeping for 310ms
		     Sleeping for 630ms
		     Sleeping for 1270ms
		     Sleeping for 2sec
		     Sleeping for 4sec")
	}

	Void testBackoffFuncTotalNapTime() {
		napTimeU  := LocalRef("napTime")
		napTime2U := LocalRef("napTime2")
		backoff := MongoBackoff {
			it.sleepFunc  = |Duration d| { napTimeU.val = napTimeU.val->plus(d)  }
			it.randomFunc = |Range r->Int| { r.max }
		}
		
		napTimeU.val = 0sec
		msgBuf := StrBuf()
		backoff.backoffFunc(|Duration d->Obj?| { napTime2U.val = d; return null }, 10sec, msgBuf)
		verifyEq(napTimeU.val,  10sec)
		verifyEq(napTime2U.val, 10sec)
		verifyEq(msgBuf.toStr,
		    "Sleeping for 10ms
		     Sleeping for 30ms
		     Sleeping for 70ms
		     Sleeping for 150ms
		     Sleeping for 310ms
		     Sleeping for 630ms
		     Sleeping for 1270ms
		     Sleeping for 2sec
		     Sleeping for 4sec")

		napTimeU.val = 0sec
		msgBuf.clear
		backoff.backoffFunc(|Duration d->Obj?| { napTime2U.val = d; return null }, 3sec, msgBuf)
		verifyEq(napTimeU.val, 3sec)
		verifyEq(napTime2U.val, 3sec)
		verifyEq(msgBuf.toStr,
		    "Sleeping for 10ms
		     Sleeping for 30ms
		     Sleeping for 70ms
		     Sleeping for 150ms
		     Sleeping for 310ms
		     Sleeping for 630ms
		     Sleeping for 1270ms
		     Sleeping for 530ms")

		napTimeU.val = 0sec
		msgBuf.clear
		backoff.backoffFunc(|Duration d->Obj?| { napTime2U.val = d; return null }, 8.7sec, msgBuf)
		verifyEq(napTimeU.val, 8.7sec)
		verifyEq(napTime2U.val, 8.7sec)
		verifyEq(msgBuf.toStr,
		    "Sleeping for 10ms
		     Sleeping for 30ms
		     Sleeping for 70ms
		     Sleeping for 150ms
		     Sleeping for 310ms
		     Sleeping for 630ms
		     Sleeping for 1270ms
		     Sleeping for 2sec
		     Sleeping for 3sec")
	}
}
