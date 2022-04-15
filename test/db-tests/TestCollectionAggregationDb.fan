using afBson

internal class TestCollectionAggregationDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"].drop(false)
		10.times |i| { collection.insert(["data":i+1]) }
	}
	
//	Void testAggregate() {
//		
//		collection.insert([
//			"_id"		: ObjectId("52769ea0f3dc6ead47c9a1b2"),
//			"author"	: "abc123",
//			"title"		: "zzz",
//			"tags"		: ["programming", "database", "mongodb"]
//		])
//		collection.insert([
//			"_id"		: ObjectId(),
//			"author"	: "SlimerDude",
//			"title"		: "Fantom Awesomeness",
//			"tags"		: ["programming", "fantom"]
//		])
//		
//		pipeline := [
//			["\$project"	: ["tags":1]],
//			["\$unwind"		: "\$tags"],
//			["\$group"		: ["_id":"\$tags", "count": ["\$sum":1]]]
//		] 
//		
//		tags := ([Str:Obj?][]) collection.aggregate(pipeline) |Cursor cursor -> Obj| {
//			cursor.toList
//		}
//		verifyEq(tags.size, 4)
//		verifyEq(tags.find { it["_id"] == "programming" }["count"], 2)
//		verifyEq(tags.find { it["_id"] == "database"    }["count"], 1)		
//	}

}
