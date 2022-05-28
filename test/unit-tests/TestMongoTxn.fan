using afBson::BsonIO
using afBson::Binary

internal class TestMongoTxn : Test {

	// My notes from the transaction spec
	// 1st cmds ONLY has startTransaction:true
	// ALL cmds have autocommit:false
	// 1st cmd MAY have readConcern - NOT inherited from MongoURL
	// ONLY commit and abort cmds have writeConcern
	// ONLY commit and abort cmds are retryable
	// ONLY commit MAY have maxTimeMS / maxCommitTimeMS
	//      commit and abort cmds are to admin db
	// track returned recoveryToken (BSON doc) and send it on commit and abort cmds
	// retry ONLY on RetryableWriteError or UnknownTransactionCommitResult label
	// Drivers ignore all abortTransaction errors

	Void testHappyCommit() {
		con := MongoConnStub().writePreamble.writeDoc(["ok":1, "recoveryToken":["judge":"death"]]).flip
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		txn	:= null as MongoTxn
		
		col.connMgr.runInTxn(null) { }
		// no Commit should have been sent, 'cos transaction was not started
		verifyEq(con.outBuf.isEmpty,	true)
		verifyEq(MongoTxn.cur, 			null)


		txnNum	:= mgr.nextTxnNum
		col.connMgr.runInTxn(null) {
			txn = MongoTxn.cur

			verifyEq(txn.status,				MongoTxn.statusStarting)

			col.insert(["j":"d"])
			
			req	:= con.readDoc
			verifyEq(txn.sess,					con.lastSess)
			verifyEq(txn.status,				MongoTxn.statusInProgress)
			verifyEq(req.keys.first,			"insert")
			verifyEq(req["txnNumber"],			txnNum)
			verifyEq(req["startTransaction"],	true)
			verifyEq(req["autocommit"],			false)
			verifyEq(req["readConcern"],		null)
			verifyEq(req["writeConcern"],		null)
			verifyEq(req["maxTimeMS"],			null)
			verifyEq(req["recoveryToken"],		null)
			
			col.insert(["j":"d"])

			req = con.readDoc
			verifyEq(txn.status,				MongoTxn.statusInProgress)
			verifyEq(req.keys.first,			"insert")
			verifyEq(req["txnNumber"],			txnNum)
			verifyEq(req["startTransaction"],	null)	// only sent in first cmd
			verifyEq(req["autocommit"],			false)
			verifyEq(req["readConcern"],		null)
			verifyEq(req["writeConcern"],		null)
			verifyEq(req["maxTimeMS"],			null)
			verifyEq(req["recoveryToken"],		Str:Obj?["judge":"death"])
		}
		
		req := con.readDoc
		verifyEq(txn.status,					MongoTxn.statusCommitted)
		verifyEq(txn.sess.isDetached,			false)
		verifyEq(req.keys.first,				"commitTransaction")
		verifyEq(req["\$db"],					"admin")
		verifyEq(req["txnNumber"],				txnNum)
		verifyEq(req["startTransaction"],		null)
		verifyEq(req["autocommit"],				false)
		verifyEq(req["readConcern"],			null)
		verifyEq(req["writeConcern"],			null)
		verifyEq(req["maxTimeMS"],				null)
		verifyEq(req["recoveryToken"],			Str:Obj?["judge":"death"])
	}
	
	Void testHappyAbort() {
		con := MongoConnStub().writePreamble.writeDoc(["ok":1, "recoveryToken":["judge":"death"]]).flip
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		txn	:= null as MongoTxn

		verifyErrMsg(Err#, "Boo!") {
			col.connMgr.runInTxn(null) {
				txn = MongoTxn.cur
				verifyEq(txn.status,	MongoTxn.statusStarting)
				throw Err("Boo!")
			}
		}
		// no Abort should have been sent, 'cos transaction was not started
		verifyEq(con.outBuf.isEmpty,	true)
		verifyEq(txn.status,			MongoTxn.statusStarting)
		
		
		verifyErrMsg(Err#, "Boo two!") {
			col.connMgr.runInTxn(["maxTimeMS":555]) {
				txn = MongoTxn.cur
				col.insert(["j":"d"])
				con.reset
				throw Err("Boo two!")
			}
		}
		req := con.readDoc
		verifyEq(txn.status,			MongoTxn.statusAborted)
		verifyEq(req.keys.first,		"abortTransaction")
		verifyEq(req["\$db"],			"admin")
		verifyEq(req["autocommit"],		false)
		verifyEq(req["maxTimeMS"],		null)	// only sent to commitTransaction
		verifyEq(req["recoveryToken"],	Str:Obj?["judge":"death"])
	}
	
	Void testOptions() {
		con := MongoConnStub().writePreamble.writeDoc(["ok":1]).flip
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		txn	:= null as MongoTxn

		col.connMgr.runInTxn([
			"readConcern"	: "judge dredd",
			"writeConcern"	: "judge anderson",
			"maxTimeMS"		: 666,
		]) {
			txn = MongoTxn.cur
			verifyEq(txn.status,			MongoTxn.statusStarting)

			col.insert(["j":"d"])
			
			req	:= con.readDoc
			verifyEq(txn.status,			MongoTxn.statusInProgress)
			verifyEq(req.keys.first,		"insert")
			verifyEq(req["readConcern"],	"judge dredd")
			verifyEq(req["writeConcern"],	null)
			verifyEq(req["maxTimeMS"],		null)
		}
		
		req := con.readDoc
		verifyEq(txn.status,				MongoTxn.statusCommitted)
		verifyEq(req.keys.first,			"commitTransaction")
		verifyEq(req["readConcern"],		null)
		verifyEq(req["writeConcern"],		"judge anderson")
		verifyEq(req["maxTimeMS"],			666)
	}
	
	Void testNoRetriesForNormalCmds() {
		con := MongoConnStub().writePreamble.writeDoc(["ok":1]).flip
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		txn	:= null as MongoTxn

		col.connMgr.runInTxn(null) {
			txn = MongoTxn.cur

			// assert IOErr retry - double Err 
			con.ress[0] = IOErr("Bad Mongo 1")
			verifyErrMsg(IOErr#, "Bad Mongo 1") {
				col.insert(["j":"d"])
			}
			verifyEq(txn.status,			MongoTxn.statusInProgress)
			verifyEq(mgr.failoverCount, 	1)	// only ONE because there was no retry
			verifyEq(txn.sess.isDirty,		true)
		}
		verifyEq(txn.status,				MongoTxn.statusCommitted)
	}	

	Void testRetryOnCommit() {
		doc := MongoConnStub().writePreamble.writeDoc(["ok":1]).flip.inBuf
		con := MongoConnStub()
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		txn	:= null as MongoTxn
		
		con.ress.add(0)
		con.ress[0] = doc
		con.ress[1] = IOErr("Bad Commit 1")
		con.ress[2] = IOErr("Bad Commit 2")
		
		mgr.writeConcern = ["judge":"dredd"]

		verifyErrMsg(IOErr#, "Bad Commit 1") {
			col.connMgr.runInTxn(null) {
				txn = MongoTxn.cur
				res := col.replace([:], [:])
				verifyEq(res["ok"], 1)
				con.outBuf.clear
			}
		}
		con.outBuf.flip
		req1 := con.readDoc(false)
		req2 := con.readDoc(false)
		verifyEq(txn.status,				MongoTxn.statusInProgress)	// because it never completed
		verifyEq(txn.sess.isDirty,			true)
		verifyEq(mgr.failoverCount,			3)	// TWO because there WAS a retry + leaseConn()
		verifyEq(req1.keys.first,			"commitTransaction")
		verifyEq(req1["writeConcern"],		Str:Obj?["judge":"dredd"])
		verifyEq(req1["writeConcern"]->get("w"),		null)
		verifyEq(req1["writeConcern"]->get("wtimeout"),	null)
		verifyEq(req2.keys.first,			"commitTransaction")
		verifyEq(req2["writeConcern"]->get("w"),		"majority")
		verifyEq(req2["writeConcern"]->get("wtimeout"),	10_000)
	}

	Void testRetryOnAbort() {
		doc := MongoConnStub().writePreamble.writeDoc(["ok":1]).flip.inBuf
		con := MongoConnStub()
		mgr := MongoConnMgrStub(con).debugOn
		col := MongoColl(mgr, "wotever")
		txn	:= null as MongoTxn
		
		con.ress.add(0)
		con.ress[0] = doc
		con.ress[1] = IOErr("Bad Abort 1")
		con.ress[2] = IOErr("Bad Abort 2")

		// Boo! because abort errors are ignored - the transaction would be left hanging and timeout - it wouldn't commit anyway
		verifyErrMsg(Err#, "Boo!") {
			col.connMgr.runInTxn(null) {
				txn = MongoTxn.cur
				col.insert(["j":"d"])
				con.outBuf.clear
				throw Err("Boo!")
			}
		}
		req := con.readDoc
		verifyEq(txn.status,					MongoTxn.statusAborted)	// because aborts don't throw errs
		verifyEq(txn.sess.isDirty,				true)
		verifyEq(mgr.failoverCount,				2)	// TWO because there WAS a retry, NO leaseConn() 'cos abort errors get swallowed
		verifyEq(req.keys.first,				"abortTransaction")
		
		// TODO retry func
		// FIXME now write a REAL DB test!
	}
}
