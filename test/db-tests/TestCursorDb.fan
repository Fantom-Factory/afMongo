
internal class TestCursorDb : MongoDbTest {
	
	MongoColl? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"]
		collection.drop
		20.times |i| { collection.insert(["data":i+1]) }
	}

	Void testCur() {
//		collection.connMgr.setDebug

		// this DID throw Err("Cannot setSession(), Session is NOT detached")
		list := collection.find(null) {
			it->batchSize = 3
//			it->maxTimeMS = "meh"
		}.toList
		
		verifyEq(list.size, 20)
	}
}
