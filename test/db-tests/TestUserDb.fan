
internal class TestUserDb : MongoDbTest {
	
	Void testAuthentication() {
		db.dropAllUsers
		zcool := db.user("ZeroCool").create("password", ["read"])
		
		db["testAuth"].insert(["wibble":"wobble"])
		
		verifyErr(MongoCmdErr#) {
			db.authenticate("ZeroCool", "whoops") |->| { }
		}
		
		// just walk through the code - we can't test actual auth without restarting MongoDB 
		db.authenticate("ZeroCool", "password") |db2->Obj?| {
			verifyEq(db2["testAuth"].findAll.first["wibble"], "wobble")
			
			// this still works 'cos we don't start Mongo with --auth 
			db2["testAuth"].insert(["wibble2":"wobble2"])
			
			return null
		}

//		// how to create a *superuser* - http://stackoverflow.com/questions/20117104/mongodb-root-user
//		mc["admin"].dropAllUsers
//		mc["admin"].user("root").create("root", ["userAdminAnyDatabase", "dbAdminAnyDatabase", "readWriteAnyDatabase"])		
	}
	
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

		zcool.revokeRoles(["dbAdmin"])
		verifyEq(zcool.roles, ["read"])
		
		zcool.drop
		verifyEq(db.userNames.size, 0)
		verifyEq(zcool.exists, false)
	}
}
