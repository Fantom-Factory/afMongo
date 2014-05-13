
internal class TestCollectionAggregationDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"]
		10.times |i| { collection.insert(["data":i+1]) }
	}
	
	Void testDistinct() {
		verifyEq(collection.size, 10)
		verifyEq(collection.distinct("data").size, 10)
		
		collection.insert(["data":3])
		collection.insert(["data":3])
		collection.insert(["data":3])
		
		verifyEq(collection.size, 13)
		verifyEq(collection.distinct("data").size, 10)
		verifyEq(collection.distinct("data", ["data": ["\$gt": 3]]).size, 7)
	}	
}
