using afBson::BsonIO
using afBson::Binary

internal class TestMongoOpSess : Test {
	
	Void testSessionSet() {
		con	:= MongoConnStub().writePreamble.writeDoc([ "foo": "bar", "ok": 1 ]).flip
		mgr := MongoConnMgrStub(con).mgr
		res := MongoOp(mgr, con, cmd("byMy")).runCommand("db")
		req := con.readDoc
		
		lsid := req["lsid"] as Str:Obj?
		ssid := lsid["id"]  as Binary
		
		verifyEq(res["foo"], "bar")
		verifyEq(req["byMy"], 1)
		verifyEq(ssid.subtype, 4)	// 4 == UUID
		verifyEq(ssid.data.toBase64, con._sess.sessionId["id"]->data->toBase64)
	}
	
	Void testSessionNoSet() {
		con	:= MongoConnStub().writePreamble.writeDoc([ "foo": "bar", "ok": 1 ]).flip
		mgr := MongoConnMgrStub(con).mgr


		// auth commands do NOT sent lsid
		res := MongoOp(mgr, con, cmd("hello")).runCommand("db")
		req := con.readDoc
		
		verifyEq(res["foo"], "bar")
		verifyEq(req["hello"], 1)
		verifyNull(req["lsid"])
		
		
		// unacknowledged writes should NOT be sent
		con.reset
		res = MongoOp(mgr, con, cmd("hello")).runCommand("db")
		req = con.readDoc
		
		verifyEq(res["foo"], "bar")
		verifyEq(req["hello"], 1)
		verifyNull(req["lsid"])
	}
	
	private [Str:Obj?] cmd(Str cmd) { Str:Obj?[:] { ordered = true }.add(cmd, 1) }
}
