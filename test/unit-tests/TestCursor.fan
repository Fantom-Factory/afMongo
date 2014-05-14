using afBson

internal class TestCursor : MongoTest {
	
	MockMongoConnection? mmc
	Cursor? cursor
	
	override Void setup() {
		mmc = MockMongoConnection()
		cursor = Cursor(mmc, Namespace("db.col"), ["wot":"ever"])
	}

	Void testAllQueryDataSent() {
		cursor.batchSize = 3 
		cursor.skip = 6 
		cursor.fieldNames = ["field1", "field2"]
		cursor.flags  = OpQueryFlags.slaveOk
		
		mmc.reply([:])
		cursor.getSome
		
		in := BsonReader(mmc.mongoIn.seek(0).in)
		in.readInteger32
		in.readInteger32
		in.readInteger32
		verifyEq(in.readInteger32, OpCode.OP_QUERY.id)
		verifyEq(in.readInteger32, OpQueryFlags.slaveOk.value)
		verifyEq(in.readCString, "db.col")
		verifyEq(in.readInteger32, 6)
		verifyEq(in.readInteger32, 3)
		verifyEq(in.readDocument, Str:Obj?["wot":"ever"])
		verifyEq(in.readDocument, Str:Obj?["field1":1, "field2":1])
	}

	Void testQueryDataCannotBeSetAfterQuery() {
		Obj? t := null
		mmc.reply([:])
		cursor.getSome
		
		t = cursor.batchSize
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.batchSize = 3
		}

		t = cursor.skip
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.skip = 3
		}

		t = cursor.fieldNames
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.fieldNames = [,]
		}

		t = cursor.flags
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.flags = OpQueryFlags.slaveOk
		}		

		t = cursor.hint
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.hint = "wotever"
		}		

		t = cursor.orderBy
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.orderBy = ["wot":"ever"]
		}		

		t = cursor.orderBy["wot"]
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.orderBy["wot"] = ["drugs?"]
		}		

		t = cursor.special
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.special = ["wot":"ever"]
		}

		t = cursor.special["wot"]
		verifyErrMsg(MongoErr#, ErrMsgs.oneShotLock_violation("Query has been sent to MongoDB")) {
			cursor.special["wot"] = ["drugs?"]
		}		
	}
}
