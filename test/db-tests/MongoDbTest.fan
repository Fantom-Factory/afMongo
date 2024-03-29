
internal class MongoDbTest : Test {
	
	MongoClient? mc
	MongoDb?	 db
	
	override Void setup() {
		mongoUri	:= `mongodb://localhost:27017`
		mc = MongoClient(mongoUri)
		db = mc["afMongoTest"]
		// not dropping the DB makes the test x10 faster!
		// but you can't delete from a capped collection!
		db.listCollectionNames.each { db[it].drop }
		Pod.of(this).log.level = LogLevel.warn
	}

	override Void teardown() {
		mc?.shutdown
	}
}
