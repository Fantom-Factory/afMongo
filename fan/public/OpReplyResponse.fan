
@NoDoc	// Boring!
class OpReplyResponse {

	const OpReplyFlags	flags
	const Int			cursorId
	const Int			cursorPos
	
	[Str:Obj?][] documents {
		internal set
	}
	
	internal new make(|This|in) { in(this) }
	
	[Str:Obj?]? document(Bool checked := true) {
		// always check for more than one, 'cos which are we to return!?
		if (documents.size > 1)
			throw MongoErr(ErrMsgs.opReply_tooMany(documents))

		return documents.getSafe(0) ?: (checked ? throw MongoErr(ErrMsgs.opReply_isEmpty) : null)
	}

	@NoDoc
	override Str toStr() {
		"${documents.size} documents, pos=${cursorPos}, cursor is " + (cursorId == 0 ? "dead" : "alive")
	}
}

@NoDoc	// Boring!
const class OpReplyFlags : Flag {
	static const OpReplyFlags none				:= OpReplyFlags(0, "None")
	
	static const OpReplyFlags cursorNotFound	:= OpReplyFlags(1.shiftl(0), "CursorNotFound")
	static const OpReplyFlags queryFailure		:= OpReplyFlags(1.shiftl(1), "QueryFailure")
	static const OpReplyFlags awaitCapable		:= OpReplyFlags(1.shiftl(3), "AwaitCapable")
	
	@NoDoc
	new make(Int flag, Str? name := null) : super(flag, name) { }
}