
internal class TestTrans : Test {
	
	Uri url := `mongodb://localhost:27017/afMongoTest`

	Void testTransientErrs() {
		col := MongoClient(url).db["X-testTrans"]
		
		// transactions are only valid on clusters
		if (col.connMgr.isStandalone)
			return typeof.pod.log.warn("Transactions are only valid on clusters - skipping test")

		col.deleteAll
		if (col.exists == false)
			col.create
		
//		col.connMgr.setDebug
		
		numRuns := 0
		col.connMgr.runInTxn(null) {
			numRuns++
			col.insert (["judge":"dredd"])
			col.replace(["judge":"dredd"], ["judge":"anderson"])
			
			if (numRuns == 1)
				throw MongoErr("Meh", ["ok":0, "code":666, "errorLabels":["TransientTransactionError"]])
		}
		
		jd := col.find(["judge":"dredd"]).toList
		ja := col.find(["judge":"anderson"]).toList
		
		verifyEq(col.size, 1)
		verifyEq(jd.size, 0)
		verifyEq(ja.size, 1)
	}
		
	Void testStdTxns() {
		col := MongoClient(url).db["X-testTrans"]
		
		// transactions are only valid on clusters
		if (col.connMgr.isStandalone)
			return typeof.pod.log.warn("Transactions are only valid on clusters - skipping test")

		col.deleteAll
		if (col.exists == false)
			col.create
		
//		col.connMgr.setDebug
		
		col.connMgr.runInTxn(null) {
			col.insert (["judge":"dredd"])
			col.replace(["judge":"dredd"], ["judge":"anderson"])
		}
		
		jd := col.findOne(["judge":"dredd"], false)
		ja := col.findOne(["judge":"anderson"], false)
		
		verifyEq(col.size, 1)
		verifyEq(jd, null)
		verifyEq(ja["judge"], "anderson")
		
		
		// now let's interrupt the trans

		col.deleteAll
		verifyErrMsg(Err#, "Poo!") {
			col.connMgr.runInTxn(null) {
				col.insert (["judge":"dredd"])
				throw Err("Poo!")	// abort, abort, abort!
				col.replace(["judge":"dredd"], ["judge":"anderson"])
			}
		}
		
		jd = col.findOne(["judge":"dredd"], false)
		ja = col.findOne(["judge":"anderson"], false)
		
		// NOTHING should have been inserted!
		verifyEq(col.size, 0)
		verifyEq(jd, null)
		verifyEq(ja, null)
	}
}
