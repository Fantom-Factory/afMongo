
internal class TestCollectionDb : MongoDbTest {
	
	Collection? collection
	
	override Void setup() {
		super.setup
		collection = db["collectionTest"]
		10.times |i| { collection.insert(["data":i+1]) }
	}

	
	Void testBasicMethods() {
		col := db["col-test"]
		
		verifyEq(col.exists, false)
		col.create
		verifyEq(col.exists, true)
		verifyEq(col.size, 0)

		verifyEq(col.insert(["milk":"juggs"]), 1)
		verifyEq(col.insert(["milk":"juggs"]), 1)
		verifyEq(col.insert(["milk":"juggs"]), 1)
		verifyEq(col.insert(["milk":"juggs"]), 1)
		
		verifyEq(col.size, 4)
		verifyEq(col.findCount(["milk":"juggs"]), 4)
		verifyEq(col.findCount(["coke":"juggs"]), 0)

		verifyEq(col.delete(["milk":"juggs"]), 1)
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"juggs"]), 3)

		verifyEq(col.update(["milk":"juggs"], ["\$set": ["milk": "muggs"]], true), 3)
		
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"juggs"]), 0)
		verifyEq(col.findCount(["milk":"muggs"]), 3)
		
		verifyEq(col.delete(["milk":"muggs"], true), 3)
		verifyEq(col.size, 0)
		verifyEq(col.exists, true)
		
		col.drop
		verifyEq(col.exists, false)
		verifyEq(col.size, 0)
		verifyEq(col.exists, false)
	}
	
	Void testCappedCollections() {
		col := db["col-cap-test"]
		
		verifyEq(col.exists, false)
		col.createCapped(2*1024, 3)
		verifyEq(col.exists, true)
		verifyEq(col.size, 0)
		
		verifyEq(col.insert(["milk":"1 pint"]), 1)
		verifyEq(col.insert(["milk":"2 pints"]), 1)
		verifyEq(col.insert(["milk":"3 pints"]), 1)
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"1 pint"]), 1)
		
		verifyEq(col.insert(["milk":"4 pints"]), 1)
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"1 pint"]), 0)
		verifyEq(col.findCount(["milk":"2 pints"]), 1)
		verifyEq(col.findCount(["milk":"3 pints"]), 1)
		verifyEq(col.findCount(["milk":"4 pints"]), 1)
		
		verifyEq(col.insert(["milk":"5 pints"]), 1)
		verifyEq(col.size, 3)
		verifyEq(col.findCount(["milk":"2 pints"]), 0)
		verifyEq(col.findCount(["milk":"3 pints"]), 1)
		verifyEq(col.findCount(["milk":"4 pints"]), 1)		
		verifyEq(col.findCount(["milk":"5 pints"]), 1)		
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
