using afBson::BsonIO

** Models an error as returned from a MongoDB Server.
const class MongoErr : Err {
	
	** The error response returned by MongoDB.
	const Str:Obj? errDoc

	** Creates a 'MongoCmdErr'.
	new make(Str msg, Str:Obj? errDoc, Err? cause := null) : super(msg, cause) {
		this.errDoc = errDoc
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

	** Returns the 'errorLabels' list. 
	Str[] errLabels() {
		errObj("errorLabels") ?: Str#.emptyList
	}
	
	private Obj? errObj(Str name) {
		errDoc[name] ?: (errDoc["writeErrors"] as [Str:Obj?][])?.first?.get(name)
	}
	
	** Pretty print the err doc.
	@NoDoc
	override Str toStr() {
		buf := StrBuf()
		buf.add("${typeof.qname}: ${msg}\n")
		buf.add("\nMongoDB says:\n")
		buf.add(BsonIO().print(errDoc, 60))
		buf.add("\n")
		buf.add("\nStack Trace:")
		return buf.toStr
	}
}
