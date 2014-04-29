
internal class TestCursorDb : MongoDbTest {

	Collection? col
	Cursor? cursor
	
	override Void setup() {
		super.setup
		col = db["cursorTest"]
		10.times |i| { col.insert(["data":i+1]) }
		cursor = Cursor(col.connection, Namespace(col.qname), [:])
	}
	
	Void testItr() {
		docs := 0
		while (cursor.hasNext) {
			doc := cursor.next
			verifyEq(cursor.index, doc["data"])
			docs++
		}
		verifyEq(docs, 10)

		docs = 0
		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.skip = 3
		while (cursor.hasNext) {
			doc := cursor.next
			verifyEq(cursor.index, doc["data"])
			docs++
		}
		verifyEq(docs, 7)
	}

	Void testFieldNames() {
		cursor.fieldNames = ["data"]
		list := cursor.toList
		verifyEq(list.size, 10)
		verifyEq(list[0]["data"], 1)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.fieldNames = ["data-2"]
		list = cursor.toList
		verifyEq(list.size, 10)
		verifyEq(list[0]["data"], null)
	}
	
	Void testToListWhole() {
		list := cursor.toList
		verifyEq(list.size, 10)
		verifyEq(list[0]["data"],  1)
		verifyEq(list[9]["data"], 10)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.skip = 3
		list = cursor.toList
		verifyEq(list.size, 7)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[6]["data"], 10)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.limit = 3
		cursor.skip = 5
		list = cursor.toList
		verifyEq(list.size, 3)
		verifyEq(list[0]["data"],  6)
		verifyEq(list[2]["data"],  8)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.limit = 30
		cursor.skip = 5
		list = cursor.toList
		verifyEq(list.size, 5)
		verifyEq(list[0]["data"],  6)
		verifyEq(list[4]["data"], 10)
	}
	
	Void testToListBatches() {
		cursor.next
		cursor.next
		list := cursor.toList
		verifyEq(list.size, 8)
		verifyEq(list[0]["data"],  3)
		verifyEq(list[7]["data"], 10)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.batchSize = 2
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 8)
		verifyEq(list[0]["data"],  3)
		verifyEq(list[7]["data"], 10)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.batchSize = 2
		cursor.skip = 1
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 7)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[6]["data"], 10)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.batchSize = 2
		cursor.skip = 1
		cursor.limit = 30
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 7)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[6]["data"], 10)

		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.batchSize = 2
		cursor.skip = 1
		cursor.limit = 5
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 3)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[2]["data"],  6)
	}

	Void testCount() {
		cursor.skip = 0
		verifyEq(cursor.index, 0)
		verifyEq(cursor.count, 10)
		verifyEq(cursor.hasNext, true)
		verifyEq(cursor.next["data"], 1)
		
		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.skip = 2
		verifyEq(cursor.index, 2)
		verifyEq(cursor.count, 10)
		verifyEq(cursor.hasNext, true)
		verifyEq(cursor.next["data"], 3)
		
		cursor = Cursor(col.connection, Namespace(col.qname), [:])
		cursor.skip = 20
		verifyEq(cursor.count, 10)
		verifyEq(cursor.index, 20)
		verifyEq(cursor.hasNext, false)

		verifyErrMsg(MongoCursorErr#, ErrMsgs.cursor_noMoreData) {
			cursor.next			
		}
	}
}
