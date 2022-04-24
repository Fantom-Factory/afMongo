
** Creates sequential sequences of 'Ints' as an alternative to using BSON 'ObjectIds'.
** 
** A Mongo Collection stores the last ID created for sequences.
** An atomic *findAndUpdate* operation both increments and returns the relevant document.
** 
** By keeping the last IDs stored in the database it ensures the IDs are persisted between system 
** restarts and that multiple clients can generate unique IDs.
** 
** While there is an overhead in generating new IDs, 'Int' IDs have the advantage of using less 
** space in indexes, being easier to work with in web applications and they're generally nicer to
** look at! 
const class MongoSeqs {
	
	** The Mongo Collection that stores the sequences.
	const MongoColl seqColl
	
	** Creates instance of 'MongoSeqs'.
	new make(MongoConnMgr connMgr, Str collName := "Seqs", Str? dbName := null) {
		this.seqColl = MongoColl(connMgr, collName, dbName ?: connMgr.database)
	}

	** Creates instance of 'MongoSeqs' with the given collection.
	new makeWithColl(MongoColl seqColl) {
		this.seqColl = seqColl
	}

	** Returns the next 'Int' ID for the given sequence name. 
	Int nextId(Str seqName) {
		seqColl.findAndUpdate(["_id":seqName], ["\$inc":["lastId":1]]) {
			it->upsert = true
		}["lastId"]
	}
	
	** Resets the last ID to zero.
	Void reset(Str seqName) {
		seqColl.update(["_id":seqName], ["\$set":["lastId":0]])
	}

	** Resets *all* the last IDs to zero.
	Void resetAll() {
		seqColl.update([:], ["\$set":["lastId":0]])
	}

	** Drops the 'MongoSeqs' collection.
	Void drop() {
		seqColl.drop(false)
	}
}
