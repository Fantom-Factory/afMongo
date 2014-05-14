
internal class TestWriteConcern : MongoTest {
	
	MockMongoConnection?	mmc
	ConnectionManager?		conMgr
	
	override Void setup() {
		mmc 	= MockMongoConnection()
		conMgr	= ConnectionManagerLocal(mmc)
	}
	
	Void testDefaultWriteConcernPropagatesToAll() {
		mmc.reply(["ok":1.0f, "version":"2.6.0"])
		mc := MongoClient(conMgr) { it.writeConcern = ["cream":"pies"] }	// not your usual concern!!!
		user := mc["any"].user("0-kool")
		coll := mc["any"].collection("zero")
		
		cream := Str:Obj?["cream":"pies"]
		jam   := Str:Obj?["jam":"pies"]

		// ---- Users -----------------------------------------------------------------------------
		
		mmc.reset.reply(["ok":1.0f])
		user.create("poo", [,])
		verifyWc(cream)
		
		mmc.reset.reply(["ok":1.0f])
		user.create("poo", [,], null, jam)
		verifyWc(jam)
		
		mmc.reset.reply(["ok":1.0f])
		user.grantRoles([,])
		verifyWc(cream)
		
		mmc.reset.reply(["ok":1.0f])
		user.grantRoles([,], jam)
		verifyWc(jam)
		
		mmc.reset.reply(["ok":1.0f])
		user.revokeRoles([,])
		verifyWc(cream)
		
		mmc.reset.reply(["ok":1.0f])
		user.revokeRoles([,], jam)
		verifyWc(jam)

		// ---- Collections -----------------------------------------------------------------------

		mmc.reset.reply(["ok":1.0f, "n":1.0f])
		coll.insert([:])
		verifyWc(cream)

		mmc.reset.reply(["ok":1.0f, "n":1.0f])
		coll.insert([:], jam)
		verifyWc(jam)
		
		mmc.reset.reply(["ok":1.0f, "n":1.0f])
		coll.delete([:])
		verifyWc(cream)

		mmc.reset.reply(["ok":1.0f, "n":1.0f])
		coll.delete([:], false, jam)
		verifyWc(jam)

		mmc.reset.reply(["ok":1.0f, "nModified":1.0f])
		coll.update([:], [:])
		verifyWc(cream)

		mmc.reset.reply(["ok":1.0f, "nModified":1.0f])
		coll.update([:], [:], null, null, jam)
		verifyWc(jam)
	}
	
	Void testWriteConcernFail() {
		mmc.reply(["ok":1.0f, "version":"2.6.0"])		
		mc := MongoClient(conMgr) { it.writeConcern = ["cream":"pies"] }	// not your usual concern!!!
		user := mc["any"].user("0-kool")
		coll := mc["any"].collection("zero")
		
		err1 := ["code":69, "errmsg":"too much cream"]
		mmc.reset.reply(["ok":0.0f, "writeErrors":[err1]])
		verifyErrMsg(MongoCmdErr#, ErrMsgs.cmd_writeErrs("when inserting into", "any.system.users", [err1])) {
			user.create("poo", [,])
		}
		
		err2 := ["code":96, "errmsg":"too little cream"]
		mmc.reset.reply(["ok":0.0f, "writeConcernError":["code":96, "errmsg":"too little cream"]])
		verifyErrMsg(MongoCmdErr#, ErrMsgs.cmd_writeErrs("when inserting into", "any.zero", [err2])) {
			coll.insert([:])
		}
	}
	
	Void verifyWc(Str:Obj? wc) {
		verifyEq(mmc.readSentDoc["writeConcern"], wc)		
	}
}
