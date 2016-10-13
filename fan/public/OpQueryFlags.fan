
** (Advanced)
** Flags to use with 'Cursors' in [Collection.find()]`Collection.find` method and the [Query Operation]`Operation.query`.
** 
** @see `https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-query`
const class OpQueryFlags : Flag {

	** No flags are set. Business as usual.
	static const OpQueryFlags none				:= OpQueryFlags(0, "None")
	
	** Tailable means cursor is not closed when the last data is retrieved.
	static const OpQueryFlags tailableCursor	:= OpQueryFlags(1.shiftl(1), "TailableCursor")
	
	** Allow query of replica slave. Normally these return an error except for namespace "local".
	static const OpQueryFlags slaveOk			:= OpQueryFlags(1.shiftl(2), "SlaveOk")
	
	** The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
	static const OpQueryFlags noCursorTimeout	:= OpQueryFlags(1.shiftl(4), "NoCursorTimeout")
	
	** Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data.
	static const OpQueryFlags awaitData			:= OpQueryFlags(1.shiftl(5), "AwaitData")
	
	** Stream the data down full blast in multiple "more" packages. 
	** Use when reading *all* the results of a query.
	static const OpQueryFlags exhaust			:= OpQueryFlags(1.shiftl(6), "Exhaust")
	
	** Get partial results from a 'mongos' if some shards are down (instead of throwing an error).
	static const OpQueryFlags partial			:= OpQueryFlags(1.shiftl(7), "Partial")
	
	@NoDoc
	new make(Int flag, Str? name) : super(flag, name) { }
}
