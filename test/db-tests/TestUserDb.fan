
internal class TestUserDb : MongoDbTest {
		
	Void testBasicMethods() {
		db.dropAllUsers
		
		zcool := db.user("ZeroCool")
		verify(zcool.info.isEmpty)
		
		verifyEq(db.userNames.size, 0)
		verifyEq(zcool.exists, false)
		
		zcool.create("wotever", ["dbAdmin"])
		
		verifyEq(db.userNames.size, 1)
		verifyEq(db.userNames, ["ZeroCool"])
		verifyEq(zcool.exists, true)
		
		verifyEq(zcool.info["db"], "afMongoTest")
		verifyEq(zcool.roles, ["dbAdmin"])
		
		zcool.grantRoles(["read"])
		verifyEq(zcool.roles, ["dbAdmin", "read"])
		Env.cur.err.printLine(zcool.info)

		zcool.revokeRoles(["dbAdmin"])
		verifyEq(zcool.roles, ["read"])
		
		zcool.drop
		verifyEq(db.userNames.size, 0)
		verifyEq(zcool.exists, false)
	}
}
