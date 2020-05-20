
** (Advanced)
** Wraps a response from MongoDB.
** 
** @See `https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-reply`.
class OpReplyResponse {

	** Response information.
	const OpReplyFlags	flags
	
	** The Mongo cursor this reply corresponds to.
	const Int			cursorId
	
	** The cursor position.
	const Int			cursorPos
	
	** The returned MongoDB BSON documents.
	[Str:Obj?][] documents {
		internal set
	}
	
	internal new make(|This|in) { in(this) }
	
	** Returns the first document. 
	** 
	** Throws an Err if 'checked' and 'documents' is empty.
	** Always throws an Err if there is more than 1 document.
	[Str:Obj?]? document(Bool checked := true) {
		// always check for more than one, 'cos which are we to return!?
		if (documents.size > 1)
			throw MongoErr(MongoErrMsgs.opReply_tooMany(documents))

		return documents.getSafe(0) ?: (checked ? throw MongoErr(MongoErrMsgs.opReply_isEmpty) : null)
	}

	@NoDoc
	override Str toStr() {
		"${documents.size} documents, pos=${cursorPos}, cursor is " + (cursorId == 0 ? "dead" : "alive")
	}
}
