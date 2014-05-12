
internal class TestRegexDb : MongoDbTest {
	
	Void testRegex() {
		caseInsen := "(?i)dude".toRegex

		verify(caseInsen.matches("dude"))
		verify(caseInsen.matches("DUDE"))
		verifyFalse(caseInsen.matches("-DUDE-"))
		
		// save and retrieve from Mongo
		col := db["Regex"]
		col.insert(["reg":caseInsen])		
		ci := col.findAll.first["reg"] as Regex

		verify(ci.matches("dude"))
		verify(ci.matches("DUDE"))
		verifyFalse(ci.matches("-DUDE-"))
	}
}
