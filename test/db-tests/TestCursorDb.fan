
internal class TestCursorDb : MongoDbTest {

	Collection? col
	Cursor? cursor
	MongoConn? connection
	
	override Void setup() {
		super.setup
		col = db["cursorTest"]
		col.drop
		10.times |i| { col.insert(["data":i+1]) }
		col.conMgr.leaseConn |c| { connection = c }	// very cheeky! Leaking refs! Make sure minPoolSize is at least 1 - else it gets closed!
		cursor = Cursor(connection, col.qname, [:])
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
		cursor = Cursor(connection, col.qname, [:])
		cursor.skip = 3
		while (cursor.hasNext) {
			doc := cursor.next
			verifyEq(cursor.index, doc["data"])
			docs++
		}
		verifyEq(docs, 7)
	}

	Void testFieldNames() {
		cursor.projection = ["data":1]
		list := cursor.toList
		verifyEq(list.size, 10)
		verifyEq(list[0]["data"], 1)

		cursor = Cursor(connection, (col.qname), [:])
		cursor.projection = ["data-2":1]
		list = cursor.toList
		verifyEq(list.size, 10)
		verifyEq(list[0]["data"], null)
	}
	
	Void testToListWhole() {
		list := cursor.toList
		verifyEq(list.size, 10)
		verifyEq(list[0]["data"],  1)
		verifyEq(list[9]["data"], 10)

		cursor = Cursor(connection, (col.qname), [:])
		cursor.skip = 3
		list = cursor.toList
		verifyEq(list.size, 7)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[6]["data"], 10)

		cursor = Cursor(connection, (col.qname), [:])
		cursor.limit = 3
		cursor.skip = 5
		list = cursor.toList
		verifyEq(list.size, 3)
		verifyEq(list[0]["data"],  6)
		verifyEq(list[2]["data"],  8)

		cursor = Cursor(connection, (col.qname), [:])
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

		cursor = Cursor(connection, (col.qname), [:])
		cursor.batchSize = 2
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 8)
		verifyEq(list[0]["data"],  3)
		verifyEq(list[7]["data"], 10)

		cursor = Cursor(connection, (col.qname), [:])
		cursor.batchSize = 2
		cursor.skip = 1
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 7)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[6]["data"], 10)

		cursor = Cursor(connection, (col.qname), [:])
		cursor.batchSize = 2
		cursor.skip = 1
		cursor.limit = 30
		cursor.next
		cursor.next
		list = cursor.toList
		verifyEq(list.size, 7)
		verifyEq(list[0]["data"],  4)
		verifyEq(list[6]["data"], 10)

		cursor = Cursor(connection, (col.qname), [:])
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
		
		cursor = Cursor(connection, (col.qname), [:])
		cursor.skip = 2
		verifyEq(cursor.index, 2)
		verifyEq(cursor.count, 8)
		verifyEq(cursor.hasNext, true)
		verifyEq(cursor.next["data"], 3)
		
		cursor = Cursor(connection, (col.qname), [:])
		cursor.skip = 20
		verifyEq(cursor.count, 0)
		verifyEq(cursor.index, 20)
		verifyEq(cursor.hasNext, false)

//		verifyErrMsg(MongoCursorErr#, MongoErrMsgs.cursor_noMoreData) {
//			cursor.next			
//		}
	}
}
