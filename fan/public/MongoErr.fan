
** As thrown by Mongo.
const class MongoErr : Err {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}

@NoDoc
const class MongoOpErr : MongoErr {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}

@NoDoc
const class MongoCmdErr : MongoErr {
	
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}

@NoDoc
const class MongoCursorErr : MongoErr {
	new make(Str msg := "", Err? cause := null) : super(msg, cause) { }
}
