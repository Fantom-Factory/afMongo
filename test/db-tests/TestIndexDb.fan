
internal class TestIndexDb : MongoDbTest {
	
	MongoCol? collection
	
	override Void setup() {
		super.setup
		collection = db["indexTest"]
		if (collection.exists) {
			collection.dropAllIndexes
			collection.drop
		}
	}
	
	Void testBasicMethods() {
		10.times |i| { collection.insertOne(["data":i+1]) }
		
		verifyEq(collection.indexNames, Str["_id_"])
		
		indat := collection.index("_data_")
		verifyEq(indat.exists, false)
		indat.create(["data":1], true)
		verifyEq(indat.exists, true)
		verifyEq(indat.info["name"], "_data_")
		
		verifyErr(MongoErr#) {
			collection.insertOne(["data":10])
			collection.insertOne(["data":10])
		}
		
		verifyEq(indat.ensure(["data":"up"], true), false)
		verifyEq(indat.ensure(["data":1], false), true)
		verifyEq(indat.ensure(["data":-1]), true)
		
		collection.insertOne(["data":10])
		
		indat.drop
		verifyEq(indat.exists, false)

		collection.dropAllIndexes
	}
	
	Void testEnsureRespectUniqueEqFalse() {
		indat := collection.index("_data_")
		indat.create(["data":1], false)
		verifyEq(indat.exists, true)
		
		verifyEq(indat.ensure(["data":1], false), false)
	}
}
