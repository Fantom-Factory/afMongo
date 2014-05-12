
internal class TestUserDb : MongoDbTest {
		
	Void testBasicMethods() {

		
//		r2:=mc["fantorepo"].user("ZeroCool").create("password", ["dbAdmin"])
		
		
		users := db.userNames
		Env.cur.err.printLine(users)
		
		r:=db.user("ZeroCool").create("wotever", ["dbAdmin"])
		Env.cur.err.printLine(r)
		
		fail()
		
	}
}
