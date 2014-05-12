
internal class MongoDbTest : MongoTest {
	
	MongoClient? mc
	Database?	 db
	
	override Void setup() {
		mc = MongoClient()
		db = mc["afMongoTest"].drop
	}

	override Void teardown() {
		mc?.shutdown
	}
	
}
