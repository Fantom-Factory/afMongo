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
		mgr := MongoConnMgrStub(con).debugOn
		col := MongoColl(mgr, "wotever")
		
		col.connMgr.runInTxn(null) { }
		// no Commit should have been sent, 'cos transaction was not started
		verifyEq(con.outBuf.isEmpty,	true)
		verifyEq(MongoTxn.cur, 			null)


		txn		:= null as MongoTxn
		txnNum := mgr.nextTxnNum
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
	
//	Void testHappyAbort() {
//		con := MongoConnStub().writePreamble.writeDoc(["ok":1, "recoveryToken":"judge death"]).flip
//		mgr := MongoConnMgrStub(con)
//		col := MongoColl(mgr, "wotever")
//
//		verifyErrMsg(Err#, "Boo!") {
//			col.connMgr.runInTxn(null) {
//				throw Err("Boo!")
//			}
//		}
//		// no Abort should have been sent, 'cos transaction was not started
//		verifyEq(con.outBuf.isEmpty,	true)
//		verifyEq(MongoTxn.cur.status,	null)
//		
//		
//		verifyErrMsg(Err#, "Boo two!") {
//			col.connMgr.runInTxn(["maxTimeMS":555]) |MongoTxn txn| {
//				col.insert(["j":"d"])
//				throw Err("Boo two!")
//			}
//		}
//		req := con.readDoc
//		verifyEq(MongoTxn.cur.status,	MongoTxn.statusAborted)
//		verifyEq(req.keys.first,		"abortTransaction")
//		verifyEq(req["\$db"],			"admin")
//		verifyEq(req["autocommit"],		false)
//		verifyEq(req["maxTimeMS"],		null)	// only sent to commitTransaction
//		verifyEq(req["recoveryToken"],	"judge death")
//	}
//	
//	Void testOptions() {
//		con := MongoConnStub().writePreamble.writeDoc(["ok":1]).flip
//		mgr := MongoConnMgrStub(con)
//		col := MongoColl(mgr, "wotever")
//
//		col.connMgr.runInTxn([
//			"readConcern"	: "judge dredd",
//			"writeConcern"	: "judge anderson",
//			"maxTimeMS"		: 666,
//		]) {
//			verifyEq(MongoTxn.cur.status,	MongoTxn.statusStarting)
//
//			col.insert(["j":"d"])
//			
//			req	:= con.readDoc
//			verifyEq(MongoTxn.cur.status,	MongoTxn.statusInProgress)
//			verifyEq(req.keys.first,		"insert")
//			verifyEq(req["readConcert"],	"judge dredd")
//			verifyEq(req["writeConcert"],	null)
//			verifyEq(req["maxTimeMS"],		null)
//		}
//		
//		req := con.readDoc
//		verifyEq(MongoTxn.cur.status,		MongoTxn.statusCommitted)
//		verifyEq(req.keys.first,			"commitTransaction")
//		verifyEq(req["readConcert"],		null)
//		verifyEq(req["writeConcert"],		"judge anderson")
//		verifyEq(req["maxTimeMS"],			666)
//	}
//	
//	Void testNoRetriesForNormalCmds() {
//		con := MongoConnStub().writePreamble.writeDoc(["ok":1, "recoveryToken":"judge death"]).flip
//		mgr := MongoConnMgrStub(con)
//		col := MongoColl(mgr, "wotever")
//
//		col.connMgr.runInTxn(null) {
//
//			// assert IOErr retry - double Err 
//			con.ress[0] = IOErr("Bad Mongo 1")
//			con.ress[1] = IOErr("Bad Mongo 2")
//			con.reset
//			verifyErrMsg(IOErr#, "Bad Mongo 1") {
//				col.insert(["j":"d"])
//			}
//			verifyEq(MongoTxn.cur.status,		MongoTxn.statusInProgress)
//			verifyEq(mgr.failoverCount, 		1)	// only ONE because there was no retry
//			verifyEq(con._getSession(false).isDirty, true)
//		}
//		verifyEq(MongoTxn.cur.status,			MongoTxn.statusCommitted)
//	}	
//
//	Void testRetryOnCommit() {
//		doc := MongoConnStub().writePreamble.writeDoc(["ok":1, "recoveryToken":"judge death"]).flip.inBuf
//		con := MongoConnStub()
//		mgr := MongoConnMgrStub(con)
//		col := MongoColl(mgr, "wotever")
//		
//		con.ress.add(0)
//		con.ress[0] = doc
//		con.ress[1] = IOErr("Bad Commit 1")
//		con.ress[2] = IOErr("Bad Commit 2")
//
//		verifyErrMsg(IOErr#, "Bad Mongo 1") {
//			col.connMgr.runInTxn(null) {
//				res := col.replace([:], [:])
//				verifyEq(res["ok"], 1)
//			}
//		}
//		req := con.readDoc
//		verifyEq(MongoTxn.cur.status,			MongoTxn.statusAborted)	// because of the Err
//		verifyEq(req.keys.first,				"commitTransaction")
//		verifyEq(mgr.failoverCount,				2)	// TWO because there WAS a retry
//		verifyEq(con._getSession(false).isDirty, true)
//	}
//
//	Void testRetryOnAbort() {
//		doc := MongoConnStub().writePreamble.writeDoc(["ok":1, "recoveryToken":"judge death"]).flip.inBuf
//		con := MongoConnStub()
//		mgr := MongoConnMgrStub(con)
//		col := MongoColl(mgr, "wotever")
//		
//		con.ress.add(0)
//		con.ress[0] = doc
//		con.ress[1] = IOErr("Bad Abort 1")
//		con.ress[2] = IOErr("Bad Abort 2")
//
//		// Boo! because abort errors are ignored - the transaction would be left hanging and timeout - it wouldn't commit anyway
//		verifyErrMsg(Err#, "Boo!") {
//			col.connMgr.runInTxn(null) {
//				col.insert(["j":"d"])
//				throw Err("Boo!")
//			}
//		}
//		req := con.readDoc
//		verifyEq(MongoTxn.cur.status,			MongoTxn.statusAborted)	// because of the Err
//		verifyEq(req.keys.first,				"abortTransaction")
//		verifyEq(mgr.failoverCount,				2)	// TWO because there WAS a retry
//		verifyEq(con._getSession(false).isDirty,	true)
//		
//		// FIXME now write a REAL DB test!
//	}
}
