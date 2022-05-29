using afBson::BsonIO
using afBson::Binary

internal class TestMongoOpRetries : Test {

	Void testTxNum() {
		con	:= MongoConnStub().writePreamble.writeDoc([ "foo": "bar", "ok": 1 ]).flip
		mgr := MongoConnMgrStub(con)

		res := MongoOp(mgr, con, cmd("insert")).runCommand("db")
		req := con.readDoc
		
		txn  := req["txnNumber"] as Int
		lsid := req["lsid"] as Str:Obj?
		ssid := lsid["id"]  as Binary
		
		verifyEq(res["foo"], "bar")
		verifyEq(req["insert"], 1)
		verifyEq(ssid.subtype, 4)	// 4 == UUID
		verifyEq(ssid.data.toBase64, con._sess.sessionId["id"]->data->toBase64)
		verifyEq(txn, 1)
		

		// test txnNum increments
		con.reset
		res = MongoOp(mgr, con, cmd("insert")).runCommand("db")
		req = con.readDoc
		txn = req["txnNumber"] as Int
		verifyEq(txn, 2)		
		
		con.reset
		res = MongoOp(mgr, con, cmd("insert")).runCommand("db")
		req = con.readDoc
		txn = req["txnNumber"] as Int
		verifyEq(txn, 3)
		
		
		// standalone servers don't do transactions
		mgr.isStandalone = true
		con.reset
		res = MongoOp(mgr, con, cmd("insert")).runCommand("db")
		req = con.readDoc
		txn = req["txnNumber"] as Int
		verifyEq(txn, null)

		
		// unacknowledged writes don't do transactions
		mgr.isStandalone = false
		con.reset
		res = MongoOp(mgr, con, cmd("insert").add("writeConcern", ["w":0])).runCommand("db")
		req = con.readDoc
		txn = req["txnNumber"] as Int
		verifyEq(txn, null)

		
		// disabled retrys don't do transactions
		mgr = MongoConnMgrStub(con, `mongodb://foo.com/bar?retryWrites=false`)
		con.reset
		res = MongoOp(mgr, con, cmd("insert")).runCommand("db")
		req = con.readDoc
		txn = req["txnNumber"] as Int
		verifyEq(txn, null)
	}
	
	Void testColCmds() {
		con	:= MongoConnStub().writePreamble.writeDoc(["ok":1, "n":69]).flip
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		
		// insert is okay
		col.insert(["j":"d"])
		txn := con.readDoc["txnNumber"]
		verifyEq(txn, 1)
		
		// multi-updates are NOT okay
		con.reset
		col.update(["judge":"dredd"], ["judge":"death"])
		txn = con.readDoc["txnNumber"]
		verifyEq(txn, null)
		
		// single-updates ARE okay
		con.reset
		col.update(["judge":"dredd"], ["judge":"dredd"]) { it->multi=false }
		txn = con.readDoc["txnNumber"]
		verifyEq(txn, 2)
		
		// multi-deletes are NOT okay
		con.reset
		col.delete(["j":"d"])
		txn = con.readDoc["txnNumber"]
		verifyEq(txn, null)
		
		// single-deleted ARE okay
		con.reset
		col.delete(["j":"d"]) { it->limit=1}
		txn = con.readDoc["txnNumber"]
		verifyEq(txn, 3)
		
		// find-and-modify is okay
		con.reset
		col.findAndUpdate(["judge":"dredd"], ["judge":"death"])
		txn = con.readDoc["txnNumber"]
		verifyEq(txn, 4)
	}

	Void testRetryWrites() {
		doc := MongoConnStub().writePreamble.writeDoc(["ok":1, "n":69]).flip.inBuf
		con	:= MongoConnStub()
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		
		// assert Errs pass through 
		con.ress[0] = Err("Boo")
		con.ress[1] = "meh"
		verifyErrMsg(Err#, "Boo") {
			col.insert(["j":"d"])
		}
		verifyEq(mgr.failoverCount, 0)
		// only IOErrs get marked as dirty
		verifyEq(con.lastSess.isDirty, false)

		
		// assert IOErr retry - recovery
		con.ress[0] = IOErr("Boo")
		con.ress[1] = doc
		con.reset
		col.insert(["j":"d"])
		verifyEq(mgr.failoverCount, 1)
		verifyEq(con.lastSess.isDirty, true)
	
	
		// assert IOErr retry - double Err 
		con.ress[0] = IOErr("Bad Mongo 1")
		con.ress[1] = IOErr("Bad Mongo 2")
		con.reset
		verifyErrMsg(IOErr#, "Bad Mongo 1") {
			col.insert(["j":"d"])
		}
		verifyEq(mgr.failoverCount, 3)	// we failover AGAIN in the retry catch block (as per spec) and AGAIN in leaseConn()
		verifyEq(con.lastSess.isDirty, true)
	

		// assert non-retryable Mongo errs pass through
		con.ress[0] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":666, "errmsg":"Judge Death"]).flip.inBuf
		con.ress[1] = doc
		con.reset
		verifyErrMsg(MongoErr#, "Command 'insert' failed. MongoDB says: Judge Death") {
			col.insert(["j":"d"])
		}
		verifyEq(mgr.failoverCount, 0)
		verifyEq(con.lastSess.isDirty, false)

		
		// assert MongoErr retry - recovery
		con.ress[0] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":10107]).flip.inBuf
		con.ress[1] = doc
		con.reset
		col.insert(["j":"d"])
		verifyEq(mgr.failoverCount, 1)
		verifyEq(con.lastSess.isDirty, false)
	

		// assert MongoErr retry - double Err 
		con.ress[0] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":10107, "errmsg":"Bad Code 1"]).flip.inBuf
		con.ress[1] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":10107, "errmsg":"Bad Code 2"]).flip.inBuf
		con.reset
		verifyErrMsg(MongoErr#, "Command 'insert' failed. MongoDB says: Bad Code 1") {
			col.insert(["j":"d"])
		}
		verifyEq(mgr.failoverCount, 2)
		verifyEq(con.lastSess.isDirty, false)
		
		
		// assert retries can be turned off
		mgr = MongoConnMgrStub(con, `mongodb://foo.com/bar?retryWrites=false`)
		col = MongoColl(mgr, "wotever")
		con.ress[0] = IOErr("Boo")
		con.ress[1] = doc
		con.reset
		verifyErrMsg(IOErr#, "Boo") {
			col.insert(["j":"d"])
		}
		verifyEq(mgr.failoverCount, 1)	// 1 for leaseConn()
		verifyEq(con.lastSess.isDirty, true)
	}
	
	Void testRetryReads() {
		doc := MongoConnStub().writePreamble.writeDoc(["ok":1, "cursor":["id":0]]).flip.inBuf
		con	:= MongoConnStub()
		mgr := MongoConnMgrStub(con)
		col := MongoColl(mgr, "wotever")
		
		// assert Errs pass through 
		con.ress[0] = Err("Boo")
		con.ress[1] = "meh"
		verifyErrMsg(Err#, "Boo") {
			col.find
		}
		verifyEq(mgr.failoverCount, 0)
		// only IOErrs get marked as dirty
		verifyEq(con.lastSess.isDirty, false)

		
		// assert IOErr retry - recovery
		con.ress[0] = IOErr("Boo")
		con.ress[1] = doc
		con.reset
		col.find
		verifyEq(mgr.failoverCount, 1)
		verifyEq(con.lastSess.isDirty, true)
	
	
		// assert IOErr retry - double Err 
		con.ress[0] = IOErr("Bad Mongo 1")
		con.ress[1] = IOErr("Bad Mongo 2")
		con.reset
		verifyErrMsg(IOErr#, "Bad Mongo 1") {
			col.find
		}
		verifyEq(mgr.failoverCount, 3)	// we failover AGAIN in the retry catch block (as per spec) and AGAIN in leaseConn()
		verifyEq(con.lastSess.isDirty, true)
	

		// assert non-retryable Mongo errs pass through
		con.ress[0] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":666, "errmsg":"Judge Death"]).flip.inBuf
		con.ress[1] = doc
		con.reset
		verifyErrMsg(MongoErr#, "Command 'find' failed. MongoDB says: Judge Death") {
			col.find
		}
		verifyEq(mgr.failoverCount, 0)
		verifyEq(con.lastSess.isDirty, false)

		
		// assert MongoErr retry - recovery
		con.ress[0] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":10107]).flip.inBuf
		con.ress[1] = doc
		con.reset
		col.find
		verifyEq(mgr.failoverCount, 1)
		verifyEq(con.lastSess.isDirty, false)
	

		// assert MongoErr retry - double Err 
		con.ress[0] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":10107, "errmsg":"Bad Code 1"]).flip.inBuf
		con.ress[1] = MongoConnStub().writePreamble.writeDoc(["ok":0, "code":10107, "errmsg":"Bad Code 2"]).flip.inBuf
		con.reset
		verifyErrMsg(MongoErr#, "Command 'find' failed. MongoDB says: Bad Code 1") {
			col.find
		}
		verifyEq(mgr.failoverCount, 2)
		verifyEq(con.lastSess.isDirty, false)
		
		
		// assert retries can be turned of) 
		mgr = MongoConnMgrStub(con, `mongodb://foo.com/bar?retryReads=false`)
		col = MongoColl(mgr, "wotever")
		con.ress[0] = IOErr("Boo")
		con.ress[1] = doc
		con.reset
		verifyErrMsg(IOErr#, "Boo") {
			col.find
		}
		verifyEq(mgr.failoverCount, 1)		// 1 for leaseConn()
		verifyEq(con.lastSess.isDirty, true)
	}
	
	private [Str:Obj?] cmd(Str cmd) { Str:Obj?[:] { ordered = true }.add(cmd, 1) }
}
