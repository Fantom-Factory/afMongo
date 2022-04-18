
internal class TestCollectionDb : MongoDbTest {
	
	MongoCol? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"]
		collection.drop
		10.times |i| { collection.insert(["data":i+1]) }
	}

	Void testFindAndUpdate() {
		collection.drop
		collection.insert([
			"author"	: "abc123",
			"score"		: 3
		])
		collection.insert([
			"author"	: "SlimerDude",
			"score"		: 5
		])
		
		// return pre-modify
		slimer := collection.findAndModify(["author":"SlimerDude"], ["\$inc":["score": 3]], false)
		verifyEq(slimer["score"], 5)
		slimer = collection.findOne(["author":"SlimerDude"])
		verifyEq(slimer["score"], 8)

		// return post-modify
		slimer = collection.findAndModify(["author":"SlimerDude"], ["\$inc":["score": 2]], true)
		verifyEq(slimer["score"], 10)
		
		verifyEq(collection.size, 2)
		slimer = collection.findAndDelete(["author":"SlimerDude"])
		verifyEq(slimer["score"], 10)
		verifyEq(collection.size, 1)
	}

	
	Void testBasicMethods() {
		col := db["col-test"]
		
		verifyEq(col.exists, false)
		col.create
		verifyEq(col.exists, true)
		verifyEq(col.size, 0)

		col.insert(["milk":"juggs"])
		col.insert(["milk":"juggs"])
		col.insert(["milk":"juggs"])
		col.insert(["milk":"juggs"])
		
		verifyEq(col.size, 4)
		verifyEq(col.findCount(["milk":"juggs"]), 4)
		verifyEq(col.findCount(["coke":"juggs"]), 0)

		verifyEq(col.delete(["milk":"juggs"]), 1)
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"juggs"]), 3)

		verifyEq(col.update(["milk":"juggs"], ["\$set": ["milk": "muggs"]])["n"], 3)
		
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"juggs"]), 0)
		verifyEq(col.findCount(["milk":"muggs"]), 3)
		
		verifyEq(col.delete(["milk":"muggs"]), 3)
		verifyEq(col.size, 0)
		verifyEq(col.exists, true)
		
		col.drop
		verifyEq(col.exists, false)
		verifyEq(col.size, 0)
		verifyEq(col.exists, false)
	}
	
//	Void testCappedCollections() {
//		col := db["col-cap-test"]
//		col.drop
//		
//		verifyEq(col.exists, false)
//		col.createCapped(2*1024, 3)
//		verifyEq(col.exists, true)
//		verifyEq(col.size, 0)
//		
//		verifyEq(col.insert(["milk":"1 pint"]), 1)
//		verifyEq(col.insert(["milk":"2 pints"]), 1)
//		verifyEq(col.insert(["milk":"3 pints"]), 1)
//		verifyEq(col.size, 3)
//		verifyEq(col.findCount(["milk":"1 pint"]), 1)
//		
//		verifyEq(col.insert(["milk":"4 pints"]), 1)
//		verifyEq(col.size, 3)
//		verifyEq(col.findCount(["milk":"1 pint"]), 0)
//		verifyEq(col.findCount(["milk":"2 pints"]), 1)
//		verifyEq(col.findCount(["milk":"3 pints"]), 1)
//		verifyEq(col.findCount(["milk":"4 pints"]), 1)
//		
//		verifyEq(col.insert(["milk":"5 pints"]), 1)
//		verifyEq(col.size, 3)
//		verifyEq(col.findCount(["milk":"2 pints"]), 0)
//		verifyEq(col.findCount(["milk":"3 pints"]), 1)
//		verifyEq(col.findCount(["milk":"4 pints"]), 1)		
//		verifyEq(col.findCount(["milk":"5 pints"]), 1)		
//	}
	
	Void testFind() {
		second := collection.find.toList[1]
		verifyEq(second["data"], 2)
		
		res := collection.find.toList
		verifyEq(10, res.size)
	}

	Void testFindOne() {
		one := collection.findOne(["data":4])
		verifyEq(one["data"], 4)

		two := collection.findOne(["data":42], false)
		verifyNull(two)
		
		verifyErrMsg(Err#, "findOne() returned ZERO documents from afMongoTest.collectionTest - [data:42]") {
			collection.findOne(["data":42])
		}

		collection.insert(["data":42])
		collection.insert(["data":42])
		verifyErrMsg(Err#, "findOne() returned multiple documents from afMongoTest.collectionTest - [data:42]") {
			collection.findOne(["data":42])
		}
	}
	
	Void testSort() {
		// test sort
		verifyEq(collection.find(null) { it->sort = ["data":  1] }.toList[0]["data"],  1)
		verifyEq(collection.find(null) { it->sort = ["data":  1] }.toList[9]["data"], 10)

		verifyEq(collection.find(null) { it->sort = ["data": -1] }.toList[0]["data"], 10)
		verifyEq(collection.find(null) { it->sort = ["data": -1] }.toList[9]["data"],  1)
		
		// test hint
		collection.index("up"  ).create(["data":  1])
		collection.index("down").create(["data": -1])
		
		verifyEq(collection.find(null) { it->hint = "up"   }.toList[0]["data"],  1)
		verifyEq(collection.find(null) { it->hint = "up"   }.toList[9]["data"], 10)
		
		verifyEq(collection.find(null) { it->hint = "down" }.toList[0]["data"], 10)
		verifyEq(collection.find(null) { it->hint = "down" }.toList[9]["data"],  1)
		
		// test invalid
		verifyErr(MongoErr#) {
			collection.find(null) { it->sort = ["data": "lavalamp"] }
		}
	}
}
