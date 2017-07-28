using afBson

internal class TestCollectionAggregationDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"].drop(false)
		10.times |i| { collection.insert(["data":i+1]) }
	}
	
	Void testGroupAgg() {
		c := collection.drop
		
		c.insert(["x":"a", "y":1])
		c.insert(["x":"a", "y":2])
		c.insert(["x":"a", "y":3])
		c.insert(["x":"b", "y":1])
		
		initial := ["count": 0.0f]
		f := "function (obj, prev) { prev.count += inc_value; }"
		
		g1 := c.group(["y"], [:], Code("function (obj, val) { return obj }"))
		verifyEq(3, g1.size)

		// with finalize
		fin := "function(doc) {doc.f = doc.count + 200; }"
		g4 := c.group(Str[,], initial, Code(f, ["inc_value":1]), ["finalize":fin])
		verifyEq(204f, g4[0]["f"])		
	}

	Void testGroup() {
		c := collection.drop
		
		c.insert(["x":"a"])
		c.insert(["x":"a"])
		c.insert(["x":"a"])
		c.insert(["x":"b"])
		
		initial := ["count": 0.0f]
		f := "function (obj, prev) { prev.count += inc_value; }"
		
		g1 := c.group(["x"], initial, Code(f, ["inc_value":1]))
		verifyEq(3f, g1[0]["count"])

		g2 := c.group(["x"], initial, Code("function (obj, prev) { prev.count += 2; }"))
		verifyEq(6f, g2[0]["count"])
		
		g3 := c.group(["x"], initial, Code(f, ["inc_value":0.5f]))
		verifyEq(1.5f, g3[0]["count"])
		
		// with finalize
		fin := "function(doc) {doc.f = doc.count + 200; }"
		g4 := c.group(Str[,], initial, Code(f, ["inc_value":1]), ["finalize":fin])
		verifyEq(204f, g4[0]["f"])		
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
		
		map := Code("function() { emit(this.user_id, 1); }")
		red := Code("function(k, vals) { return 1; }")
		res := c.mapReduce(map, red, ["out":"whoopie"])

		mrcoll := db[res["result"]]
		verifyNotNull(mrcoll.findOne(["_id": 1]))
		verifyNotNull(mrcoll.findOne(["_id": 2]))
		
		mrcoll.drop
		c.drop
		c.insert(["user_id": 1])
		c.insert(["user_id": 2])
		c.insert(["user_id": 3])
		
		res = c.mapReduce(map, red, ["out":"whoopie", "query": ["user_id": ["\$gt": 1]]])
		mrcoll = db[res["result"]]
		verifyEq(2, mrcoll.size)
		verifyNull(mrcoll.get(1, false))
		verifyNotNull(mrcoll.get(2))
		verifyNotNull(mrcoll.get(3))
	}
}
