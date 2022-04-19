
** Models an error as returned from a MongoDB Server.
const class MongoErr : Err {
	
	** The error response returned by MongoDB.
	const Str:Obj? errRes

	** Creates a 'MongoCmdErr'.
	new make(Str msg, Str:Obj? errRes, Err? cause := null) : super(msg, cause) {
		this.errRes = errRes
	}
	
	** Returns the 'code', if it exists.
	Int? code() {
		errObj("code")
	}

	** Returns the 'codeName', if it exists. 
	Str? codeName() {
		errObj("codeName")
	}

	** Returns the 'errmsg', if it exists. 
	Str? errMsg() {
		errObj("errmsg")
	}
	
	// TODO BSON PrettyPrint the errDoc in toStr()

	private Obj? errObj(Str name) {
		errRes[name] ?: errRes["writeErrors"]?->first?->get(name)
	}
}
