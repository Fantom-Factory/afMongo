
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
		errRes["code"]
	}

	** Returns the 'codeName', if it exists. 
	Str? codeName() {
		errRes["codeName"]
	}

	** Returns the 'errmsg', if it exists. 
	Str? errMsg() {
		errRes["errmsg"]
	}
}
