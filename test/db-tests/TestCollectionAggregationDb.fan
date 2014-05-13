using afBson

internal class TestCollectionAggregationDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"].drop(false)
		10.times |i| { collection.insert(["data":i+1]) }
	}
	
	Void testAggregate() {
		
		collection.insert([
			"_id"		: ObjectId("52769ea0f3dc6ead47c9a1b2"),
			"author"	: "abc123",
			"title"		: "zzz",
			"tags"		: ["programming", "database", "mongodb"]
		])
		collection.insert([
			"_id"		: ObjectId(),
			"author"	: "SlimerDude",
			"title"		: "Fantom Awesomeness",
			"tags"		: ["programming", "fantom"]
		])
		
		pipeline := [
			["\$project"	: ["tags":1]],
			["\$unwind"		: "\$tags"],
			["\$group"		: ["_id":"\$tags", "count": ["\$sum":1]]]
		] 
		
		tags := collection.aggregate(pipeline)
		verifyEq(tags.size, 4)
		verifyEq(tags.find { it["_id"] == "programming" }["count"], 2)
		verifyEq(tags.find { it["_id"] == "database"    }["count"], 1)

		tags = collection.aggregateCursor(pipeline) |Cursor cursor -> Obj| {
			cursor.toList
		}
		verifyEq(tags.size, 4)
		verifyEq(tags.find { it["_id"] == "programming" }["count"], 2)
		verifyEq(tags.find { it["_id"] == "database"    }["count"], 1)		
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
	
	Void testMapReduce() {
		c := collection.drop
		
		c.insert(["user_id": 1])
		c.insert(["user_id": 2])
		
		map := "function() { emit(this.user_id, 1); }"
		red := "function(k, vals) { return 1; }"
		res := c.mapReduce(map, red, "whoopie")

		mrcoll := db[res["result"]]
		verifyNotNull(mrcoll.findOne(["_id": 1]))
		verifyNotNull(mrcoll.findOne(["_id": 2]))
		
		mrcoll.drop
		c.drop
		c.insert(["user_id": 1])
		c.insert(["user_id": 2])
		c.insert(["user_id": 3])
		
		res = c.mapReduce(map, red, "whoopie", ["query": ["user_id": ["\$gt": 1]]])
		mrcoll = db[res["result"]]
		verifyEq(2, mrcoll.size)
		verifyNull(mrcoll.get(1, false))
		verifyNotNull(mrcoll.get(2))
		verifyNotNull(mrcoll.get(3))
	}
}
