
internal class TestMongoConnUrl : Test {
	
	private static LogRec[] logs() { ulogs.val }
	private static const Unsafe ulogs := Unsafe(LogRec[,])
	private static const |LogRec rec| handler := |LogRec rec| { logs.add(rec) }
	
	override Void setup() {
		logs.clear
		Log.addHandler(handler)
	}
	
	override Void teardown() {
		Log.removeHandler(handler)		
	}
	
	Void testMongoUriUserCreds() {		
		verifyErrMsg(ArgErr#, "Mongo connection URIs must start with the scheme 'mongodb://' - dude://wotsup?") {
			conUrl := MongoConnUrl(`dude://wotsup?`)
		}
		
		verifyErrMsg(ArgErr#, "Either both the username and password should be provided, or neither. username=user, password=null, url=mongodb://user@wotever") {
			conUrl := MongoConnUrl(`mongodb://user@wotever`)
		}

		verifyErrMsg(ArgErr#, "Either both the username and password should be provided, or neither. username=null, password=pass, url=mongodb://:pass@wotever") {
			conUrl := MongoConnUrl(`mongodb://:pass@wotever`)
		}

		conUrl := MongoConnUrl(`mongodb://user:pass@wotever/puppies`)
		verifyEq(conUrl.mongoCreds.source, "puppies")
		verifyEq(conUrl.mongoCreds.username, "user")
		verifyEq(conUrl.mongoCreds.password, "pass")

		verify(logs.isEmpty)
	}	

	Void testMongoUriAuthMech() {		
		conUrl := MongoConnUrl(`mongodb://user:pass@wotever/puppies?authSource=kinesis&authMechanism=pacificRim&authMechanismProperties=cannon:Plasmacaster,,,turbine:NuclearVortex`)
		verifyEq(conUrl.mongoCreds.mechanism,	"pacificRim")
		verifyEq(conUrl.mongoCreds.source, 		"kinesis")
		verifyEq(conUrl.mongoCreds.username,	"user")
		verifyEq(conUrl.mongoCreds.password,	"pass")
		verifyEq(conUrl.mongoCreds.props,		Str:Obj?[
			"cannon"	: "Plasmacaster",
			"turbine"	: "NuclearVortex",
		])

		verify(logs.isEmpty)
	}
	
	Void testMongoUriPoolOptions() {
		verifyErrMsg(ArgErr#, "minPoolSize must be >= 0, val=-1, uri=mongodb://wotever?minPoolSize=-1") {
			conUrl := MongoConnUrl(`mongodb://wotever?minPoolSize=-1`)
		}
		
		verifyErrMsg(ArgErr#, "maxPoolSize must be >= 1, val=0, uri=mongodb://wotever?maxPoolSize=0") {
			conUrl := MongoConnUrl(`mongodb://wotever?maxPoolSize=0`)
		}
		
		verifyErrMsg(ArgErr#, "Minimum number of connections must not be greater than the maximum! min=2, max=1, url=mongodb://wotever?minPoolSize=2&maxPoolSize=1") {
			conUrl := MongoConnUrl(`mongodb://wotever?minPoolSize=2&maxPoolSize=1`)
		}
		
		verifyErrMsg(ArgErr#, "waitQueueTimeoutMS must be >= 0, val=-1, uri=mongodb://wotever?waitQueueTimeoutMS=-1") {
			conUrl := MongoConnUrl(`mongodb://wotever?waitQueueTimeoutMS=-1`)
		}		

		verifyErrMsg(ArgErr#, "maxIdleTimeMS must be >= 0, val=-3, uri=mongodb://wotever?maxIdleTimeMS=-3") {
			conUrl := MongoConnUrl(`mongodb://wotever?maxIdleTimeMS=-3`)
		}

		conUrl := MongoConnUrl(`mongodb://wotever?minPoolSize=2&maxPoolSize=15&waitQueueTimeoutMS=3000&maxIdleTimeMS=1200`)
		verifyEq(conUrl.minPoolSize,  2)
		verifyEq(conUrl.maxPoolSize, 15)
		verifyEq(conUrl.waitQueueTimeout, 3sec)
		verifyEq(conUrl.maxIdleTime, 1.2sec)
		verify(logs.isEmpty)
	}

	Void testMongoUriConnectionOptions() {
		verifyErrMsg(ArgErr#, "connectTimeoutMS must be >= 0, val=-1, uri=mongodb://wotever?connectTimeoutMS=-1") {
			conUrl := MongoConnUrl(`mongodb://wotever?connectTimeoutMS=-1`)
		}
		
		verifyErrMsg(ArgErr#, "socketTimeoutMS must be >= 0, val=-1, uri=mongodb://wotever?socketTimeoutMS=-1") {
			conUrl := MongoConnUrl(`mongodb://wotever?socketTimeoutMS=-1`)
		}
		
		conUrl := MongoConnUrl(`mongodb://wotever?connectTimeoutMS=2000&socketTimeoutMS=3000`)
		verifyEq(conUrl.connectTimeout, 2sec)
		verifyEq(conUrl.socketTimeout,  3sec)
		verify(logs.isEmpty)
	}
	
	Void testWarningsForUnknownQueryOptions() {
		conUrl := MongoConnUrl(`mongodb://wotever?dude=wotever&dude2`)
		verifyEq(logs[0].msg, "Unknown option in Mongo connection URL: dude=wotever")
		verifyEq(logs[1].msg, "Unknown option in Mongo connection URL: dude2=true")
	}

	Void testMongoLabConnectionStr() {
		conUrl	:= MongoConnUrl(`mongodb://user:pass@ds999999-a0.mlab.com:55555,@ds999999-a1.mlab.com:44444/stackhub?replicaSet=rs-ds059296`)
		verifyEq(conUrl.mongoCreds.source,		"stackhub")
		verifyEq(conUrl.mongoCreds.username,	"user")
		verifyEq(conUrl.mongoCreds.password,	"pass")
		verifyEq(conUrl.connectionUrl.host,		"ds999999-a0.mlab.com:55555,@ds999999-a1.mlab.com")
		verifyEq(conUrl.connectionUrl.port,		44444)

		hg := conUrl.connectionUrl.host.split(',')
		hostList := (Mongo4x4[]) hg.map { Mongo4x4(it, false, null, [,], this.typeof.pod.log) }
		hostList.last.port = conUrl.connectionUrl.port ?: 27017
		verifyEq(hostList[0].address,	"ds999999-a0.mlab.com")
		verifyEq(hostList[0].port, 		55555)
		verifyEq(hostList[1].address,	"ds999999-a1.mlab.com")
		verifyEq(hostList[1].port, 		44444)
	}
	
	Void testWriteConcern() {
		// test default
		conUrl := MongoConnUrl(`mongodb://wotever`)
		verifyEq(conUrl.writeConcern, null)

		// write concern
		conUrl = MongoConnUrl(`mongodb://wotever?w=-1`)
		verifyEq(conUrl.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", -1))
		
		conUrl = MongoConnUrl(`mongodb://wotever?w=0`)
		verifyEq(conUrl.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 0))
		
		conUrl = MongoConnUrl(`mongodb://wotever?w=1`)
		verifyEq(conUrl.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 1))

		// write timeout
		conUrl = MongoConnUrl(`mongodb://wotever?wtimeoutMS=2000`)
		verifyEq(conUrl.writeConcern, Str:Obj?[:] { it.ordered=true }.add("wtimeout", 2000))
		
		// journal
		conUrl = MongoConnUrl(`mongodb://wotever?journal=true`)
		verifyEq(conUrl.writeConcern, Str:Obj?[:] { it.ordered=true }.add("j", true))
		
		// all options
		conUrl = MongoConnUrl(`mongodb://wotever?w=3&wtimeoutMS=1234&journal=true`)
		verifyEq(conUrl.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 3).add("wtimeout", 1234).add("j", true))

		verify(logs.isEmpty)
	}
	
	Void testAppName() {
		conUrl	:= MongoConnUrl(`mongodb://wotever?appname=`)
		verifyEq(conUrl.appName, null)

		conUrl	= MongoConnUrl(`mongodb://wotever?appname=WattsApp`)
		verifyEq(conUrl.appName, "WattsApp")

		conUrl	= MongoConnUrl(`mongodb://wotever?appname=someVeryLongApplicationNameThatShouldExceedOneHundredAndTwentyEightBytesBecauseThatWouldCauseItToBeTruncatedTherebyCreatingAValidTest`)
		verifyEq(conUrl.appName, "someVeryLongApplicationNameThatShouldExceedOneHundredAndTwentyEightBytesBecauseThatWouldCauseItToBeTruncatedTherebyCreatingAVali")
	}
	
	Void testCompressors() {
		conUrl	:= MongoConnUrl(`mongodb://wotever`)
		verifyEq(conUrl.compressors, ["zlib"])
	
		conUrl	= MongoConnUrl(`mongodb://wotever?compressors=`)
		verifyEq(conUrl.compressors, Str[,])
		
		conUrl	= MongoConnUrl(`mongodb://wotever?compressors=snappy, zlib`)
		verifyEq(conUrl.compressors, ["zlib"])
		
		conUrl	= MongoConnUrl(`mongodb://wotever`)
		verifyEq(conUrl.zlibCompressionLevel, null)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?zlibCompressionLevel`)
		verifyEq(conUrl.zlibCompressionLevel, null)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?zlibCompressionLevel=-1`)
		verifyEq(conUrl.zlibCompressionLevel, null)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?zlibCompressionLevel=0`)
		verifyEq(conUrl.zlibCompressionLevel, 0)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?zlibCompressionLevel=6`)
		verifyEq(conUrl.zlibCompressionLevel, 6)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?zlibCompressionLevel=9`)
		verifyEq(conUrl.zlibCompressionLevel, 9)
		
		verifyErrMsg(ArgErr#, "zlibCompressionLevel must be >= -1, val=-2, uri=mongodb://wotever?zlibCompressionLevel=-2") {
			conUrl = MongoConnUrl(`mongodb://wotever?zlibCompressionLevel=-2`)
		}
	
		verifyErrMsg(ArgErr#, "zlibCompressionLevel must be <= 9, val=10, uri=mongodb://wotever?zlibCompressionLevel=10") {
			conUrl = MongoConnUrl(`mongodb://wotever?zlibCompressionLevel=10`)
		}
	}
	
	Void testRetryWrites() {
		conUrl	:= MongoConnUrl(`mongodb://wotever`)
		verifyEq(conUrl.retryWrites, true)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?retryWrites=true`)
		verifyEq(conUrl.retryWrites, true)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?retryWrites=1`)
		verifyEq(conUrl.retryWrites, true)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?retryWrites=false`)
		verifyEq(conUrl.retryWrites, false)
	}	

	Void testRetryReads() {
		conUrl	:= MongoConnUrl(`mongodb://wotever`)
		verifyEq(conUrl.retryReads, true)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?retryReads=true`)
		verifyEq(conUrl.retryReads, true)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?retryReads=1`)
		verifyEq(conUrl.retryReads, true)
	
		conUrl	= MongoConnUrl(`mongodb://wotever?retryReads=false`)
		verifyEq(conUrl.retryReads, false)
	}
}
