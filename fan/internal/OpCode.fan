
** A list of operation codes as supported by MongoDB.
** 
** @see `http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes`
internal enum class OpCode {
	
	** Signifies a reply from MongoDB.
	OP_REPLY		(1),
	
	** Deprecated, do not use.
	OP_MSG			(1000),
	
	** Deprecated in MongoDB 2.6, do not use.
	OP_UPDATE		(2001),
	
	** Insert a new document.
	** Deprecated in MongoDB 2.6, do not use.
	OP_INSERT		(2002),
	
	** Deprecated, do not use.
	** Formerly used for 'OP_GET_BY_OID'.
	RESERVED		(2003),
	
	** Queries a collection.
	OP_QUERY		(2004),
	
	** Gets more data from a query.
	OP_GET_MORE		(2005),
	
	** Delete documents.
	** Deprecated in MongoDB 2.6, do not use.
	OP_DELETE		(2006),
	
	** Tell MongoDB the cursors are no longer used.
	OP_KILL_CURSORS	(2007);

	const Int id

	private new make(Int id) {
		this.id = id
	}

	** Returns the BSON type for the given id.
	** Throws an 'ArgErr' if invalid.
	static new fromId(Int id, Bool checked := true) {
		OpCode.vals.find { it.id == id } ?: (checked ? throw ArgErr(ErrMsgs.opCode_unknownId(id)) : null)
	}
}
