using inet

internal class MongoTest : Test {
	
	Void verifyMongoErrMsg(Str errMsg, |Obj| func) {
		verifyErrMsg(MongoErr#, errMsg, func)
	}
}
