
internal class TestCollectionDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"]
		10.times |i| { collection.insert(["data":i+1]) }
	}

	Void testFind() {
		second := collection.find([:]) |cursor| {
			first  := cursor.next
			second := cursor.next
			return second
		} as Str:Obj?
		verifyEq(second["data"], 2)
	}

	Void testFindOne() {
		one := collection.findOne(["data":4])
		verifyEq(one["data"], 4)

		two := collection.findOne(["data":42], false)
		verifyNull(two)
		
		verifyErrMsg(MongoErr#, ErrMsgs.collection_findOneIsEmpty(collection.qname, ["data":42])) {
			collection.findOne(["data":42])
		}

		collection.insert(["data":42])
		collection.insert(["data":42])
		verifyErrMsg(MongoErr#, ErrMsgs.collection_findOneHasMany(collection.qname, 2, ["data":42])) {
			collection.findOne(["data":42])
		}

		collection.insert(["data":42])
		verifyErrMsg(MongoErr#, ErrMsgs.collection_findOneHasMany(collection.qname, 3, ["data":42])) {
			collection.findOne(["data":42])
		}
	}
	
}
