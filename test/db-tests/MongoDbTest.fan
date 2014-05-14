
internal class MongoDbTest : MongoTest {
	
	MongoClient? mc
	Database?	 db
	
	override Void setup() {
		mc = MongoClient()
		db = mc["afMongoTest"]
		// not dropping the DB makes the test x10 faster!
		db.collectionNames.each { db[it].drop }
	}

	override Void teardown() {
		mc?.shutdown
	}
	
}
