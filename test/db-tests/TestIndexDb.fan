
internal class TestIndexDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["indexTest"]
	}
	
	Void testBasicMethods() {
		10.times |i| { collection.insert(["data":i+1]) }
		
		verifyEq(collection.indexNames, Obj?["_id_"])
		
		indat := collection.index("_data_")
		verifyEq(indat.exists, false)
		indat.create(["data":1], true)
		verifyEq(indat.exists, true)
		verifyEq(indat.info["name"], "_data_")
		
		verifyErr(MongoCmdErr#) {			
			// FIXME Mongo3.x no longer throws an err here until we ensure the index
			collection.insert(["data":10])
//			collection.insert(["data":10], ["w": 1, "wtimeout": 0, "j": true])
		}
		
		verifyEq(indat.ensure(["data":"up"], true), false)
		verifyEq(indat.ensure(["data":1], false), true)
		verifyEq(indat.ensure(["data":-1]), true)
		
		collection.insert(["data":10])
		
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
