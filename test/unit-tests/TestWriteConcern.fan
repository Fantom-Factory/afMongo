
internal class TestWriteConcern : MongoTest {
	
	MockMongoConnection?	mmc
	ConnectionManager?		conMgr
	
	override Void setup() {
		mmc 	= MockMongoConnection()
		conMgr	= ConnectionManagerLocal(mmc)
	}
	
	Void testDefaultWriteConcernPropagatesToAll() {
		mc := MongoClient(conMgr) { it.writeConcern = ["cream":"pies"] }	// not your usual concern!!!
		user := mc["any"].user("0-kool")
		coll := mc["any"].collection("zero")
		
		cream := Str:Obj?["cream":"pies"]
		jam   := Str:Obj?["jam":"pies"]

		// ---- Users -----------------------------------------------------------------------------
		
		mmc.reply(["ok":1.0f])
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
		mc := MongoClient(conMgr) { it.writeConcern = ["cream":"pies"] }	// not your usual concern!!!
		user := mc["any"].user("0-kool")
		coll := mc["any"].collection("zero")
		
		mmc.reply(["ok":1.0f])
		user.create("poo", [,])
		verifyEq(mmc.readSentDoc["writeConcern"], Str:Obj?["cream":"pies"])
		
		fail
	}
	
	Void verifyWc(Str:Obj? wc) {
		verifyEq(mmc.readSentDoc["writeConcern"], wc)		
	}
}
