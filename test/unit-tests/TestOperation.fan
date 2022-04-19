//using afBson
//
//internal class TestOperation : MongoTest {
//
//	MockMongoConnection? mmc
//	
//	override Void setup() {
//		mmc = MockMongoConnection()
//	}
//	
//	Void testReplyInvalidOpCode() {
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(42)
//
//		verifyErrMsg(MongoOpErr#, MongoErrMsgs.operation_resOpCodeInvalid(42)) {
//			Operation(mmc).readReply(-1)
//		}
//	}
//
//	Void testReplyIdMismatch() {
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(42)
//		mmc.replyOut.writeInteger32(OpCode.OP_REPLY.id)
//
//		verifyErrMsg(MongoOpErr#, MongoErrMsgs.operation_resIdMismatch(41, 42)) {
//			Operation(mmc).readReply(41)
//		}		
//	}
//
//	Void testReplyQueryFailure1() {
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(42)
//		mmc.replyOut.writeInteger32(OpCode.OP_REPLY.id)
//
//		mmc.replyOut.writeInteger32(OpReplyFlags.queryFailure.value)
//		mmc.replyOut.writeInteger64(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(0)
//				
//		verifyErrMsg(MongoOpErr#, MongoErrMsgs.operation_queryFailure(null)) {
//			Operation(mmc).readReply(42)
//		}		
//	}
//
//	Void testReplyQueryFailure2() {
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(42)
//		mmc.replyOut.writeInteger32(OpCode.OP_REPLY.id)
//
//		mmc.replyOut.writeInteger32(OpReplyFlags.queryFailure.value)
//		mmc.replyOut.writeInteger64(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(1)
//		mmc.replyOut.writeDocument(["\$err":"You Mong!"])
//				
//		verifyErrMsg(MongoOpErr#, MongoErrMsgs.operation_queryFailure("You Mong!")) {
//			Operation(mmc).readReply(42)
//		}		
//	}
//
//	Void testReplyQueryFailure3() {
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(42)
//		mmc.replyOut.writeInteger32(OpCode.OP_REPLY.id)
//
//		mmc.replyOut.writeInteger32(OpReplyFlags.queryFailure.value)
//		mmc.replyOut.writeInteger64(-1)
//		mmc.replyOut.writeInteger32(-1)
//		mmc.replyOut.writeInteger32(1)
//		mmc.replyOut.writeDocument(["\$err":69])
//				
//		verifyErrMsg(MongoOpErr#, MongoErrMsgs.operation_queryFailure("69")) {
//			Operation(mmc).readReply(42)
//		}		
//	}
//	
//	Void testCmdNotOrdered() {
//		verifyErrMsg(ArgErr#, MongoErrMsgs.operation_cmdNotOrdered("wotever", ["wot":1, "ever":2])) {
//			Operation(mmc).runCommand("wotever", ["wot":1, "ever":2])
//		}
//	}
//
//	Void testCmdErr1() {
//		// worst case scenario, no data in or out! Make sure we handle null values.
//		mmc.reply([:])
//		verifyErrMsg(MongoCmdErr#, MongoErrMsgs.operation_cmdFailed("null", [:].toStr)) {
//			Operation(mmc).runCommand("nuffin", [:])
//		}
//	}
//
//	Void testCmdErr2() {
//		// test the err msg is reported in Err
//		mmc.reply(["errmsg":"You can't kill me!"])
//		verifyErrMsg(MongoCmdErr#, MongoErrMsgs.operation_cmdFailed("shutdown", "You can't kill me!")) {
//			Operation(mmc).runCommand("kill", ["shutdown":1])
//		}
//	}
//}
