
** (Advanced)
** Flags set in a [Reply Response]`OpReplyResponse` from MongoDB.
** 
** @see `https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-reply`
const class OpReplyFlags : Flag {
	
	** No flags are set. Business as usual.
	static const OpReplyFlags none				:= OpReplyFlags(0, "None")
	
	** Set when the cursor id is not valid at the server. 
	static const OpReplyFlags cursorNotFound	:= OpReplyFlags(1.shiftl(0), "CursorNotFound")
	
	** Set when when query failed. Results consist of one document containing an '$err' field describing the failure.
	static const OpReplyFlags queryFailure		:= OpReplyFlags(1.shiftl(1), "QueryFailure")
	
	** Set when the server supports the 'AwaitData' Query option.
	static const OpReplyFlags awaitCapable		:= OpReplyFlags(1.shiftl(3), "AwaitCapable")
	
	@NoDoc
	new make(Int flag, Str? name := null) : super(flag, name) { }
}