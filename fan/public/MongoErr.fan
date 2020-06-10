
** As thrown by Mongo.
const class MongoErr : Err {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}

@NoDoc
const class MongoOpErr : MongoErr {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}

@NoDoc
const class MongoIoErr : MongoErr {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}

** Wraps errors returned from a MongoDB Cmd.
const class MongoCmdErr : MongoErr {
	
	** The list of errors returned by MongoDB.
	const [Str:Obj?][] errs := [Str:Obj?][,]

	** Creates a 'MongoCmdErr'.
	new make(Str msg, [Str:Obj?][] errs, Err? cause := null) : super(msg, cause) {
		this.errs = errs
	}
	
	** Returns the first 'code' in the list of Mongo errors, if it exists.
	Int? code() {
		errs.first["code"]
	}

	** Returns the first 'errmsg' in the list of Mongo errors, if it exists. 
	Str? errmsg() {
		errs.first["errmsg"]
	}
}

@NoDoc
const class MongoCursorErr : MongoErr {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}
