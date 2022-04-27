
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
			conMgr := MongoConnUrl(`dude://wotsup?`)
		}
		
		verifyErrMsg(ArgErr#, "Either both the username and password should be provided, or neither. username=user, password=null, url=mongodb://user@wotever") {
			conMgr := MongoConnUrl(`mongodb://user@wotever`)
		}

		verifyErrMsg(ArgErr#, "Either both the username and password should be provided, or neither. username=null, password=pass, url=mongodb://:pass@wotever") {
			conMgr := MongoConnUrl(`mongodb://:pass@wotever`)
		}

		conMgr := MongoConnUrl(`mongodb://user:pass@wotever/puppies`)
		verifyEq(conMgr.mongoCreds.source, "puppies")
		verifyEq(conMgr.mongoCreds.username, "user")
		verifyEq(conMgr.mongoCreds.password, "pass")

		verify(logs.isEmpty)
	}	

	Void testMongoUriAuthMech() {		
		conMgr := MongoConnUrl(`mongodb://user:pass@wotever/puppies?authSource=kinesis&authMechanism=pacificRim&authMechanismProperties=cannon:Plasmacaster,,,turbine:NuclearVortex`)
		verifyEq(conMgr.mongoCreds.mechanism,	"pacificRim")
		verifyEq(conMgr.mongoCreds.source, 		"kinesis")
		verifyEq(conMgr.mongoCreds.username,	"user")
		verifyEq(conMgr.mongoCreds.password,	"pass")
		verifyEq(conMgr.mongoCreds.props,		Str:Obj?[
			"cannon"	: "Plasmacaster",
			"turbine"	: "NuclearVortex",
		])

		verify(logs.isEmpty)
	}
	
	Void testMongoUriPoolOptions() {
		verifyErrMsg(ArgErr#, "minPoolSize must be greater than zero! val=-1, uri=mongodb://wotever?minPoolSize=-1") {
			conMgr := MongoConnUrl(`mongodb://wotever?minPoolSize=-1`)
		}
		
		verifyErrMsg(ArgErr#, "maxPoolSize must be greater than one! val=0, uri=mongodb://wotever?maxPoolSize=0") {
			conMgr := MongoConnUrl(`mongodb://wotever?maxPoolSize=0`)
		}
		
		verifyErrMsg(ArgErr#, "Minimum number of connections must not be greater than the maximum! min=2, max=1, url=mongodb://wotever?minPoolSize=2&maxPoolSize=1") {
			conMgr := MongoConnUrl(`mongodb://wotever?minPoolSize=2&maxPoolSize=1`)
		}
		
		verifyErrMsg(ArgErr#, "waitQueueTimeoutMS must be greater than zero! val=-1, uri=mongodb://wotever?waitQueueTimeoutMS=-1") {
			conMgr := MongoConnUrl(`mongodb://wotever?waitQueueTimeoutMS=-1`)
		}

		conMgr := MongoConnUrl(`mongodb://wotever?minPoolSize=2&maxPoolSize=15&waitQueueTimeoutMS=3000`)
		verifyEq(conMgr.minPoolSize,  2)
		verifyEq(conMgr.maxPoolSize, 15)
		verifyEq(conMgr.waitQueueTimeout, 3sec)
		verify(logs.isEmpty)
	}

	Void testMongoUriConnectionOptions() {
		verifyErrMsg(ArgErr#, "connectTimeoutMS must be greater than zero! val=-1, uri=mongodb://wotever?connectTimeoutMS=-1") {
			conMgr := MongoConnUrl(`mongodb://wotever?connectTimeoutMS=-1`)
		}
		
		verifyErrMsg(ArgErr#, "socketTimeoutMS must be greater than zero! val=-1, uri=mongodb://wotever?socketTimeoutMS=-1") {
			conMgr := MongoConnUrl(`mongodb://wotever?socketTimeoutMS=-1`)
		}
		
		conMgr := MongoConnUrl(`mongodb://wotever?connectTimeoutMS=2000&socketTimeoutMS=3000`)
		verifyEq(conMgr.connectTimeout, 2sec)
		verifyEq(conMgr.socketTimeout,  3sec)
		verify(logs.isEmpty)
	}
	
	Void testWarningsForUnknownQueryOptions() {
		conMgr := MongoConnUrl(`mongodb://wotever?dude=wotever&dude2`)
		verifyEq(logs[0].msg, "Unknown option in Mongo connection URL: dude=wotever")
		verifyEq(logs[1].msg, "Unknown option in Mongo connection URL: dude2=true")
	}

	Void testMongoLabConnectionStr() {
		conMgr	:= MongoConnUrl(`mongodb://user:pass@ds999999-a0.mlab.com:55555,@ds999999-a1.mlab.com:44444/stackhub?replicaSet=rs-ds059296`)
		verifyEq(conMgr.mongoCreds.source,		"stackhub")
		verifyEq(conMgr.mongoCreds.username,	"user")
		verifyEq(conMgr.mongoCreds.password,	"pass")
		verifyEq(conMgr.connectionUrl.host,		"ds999999-a0.mlab.com:55555,@ds999999-a1.mlab.com")
		verifyEq(conMgr.connectionUrl.port,		44444)

		hg := conMgr.connectionUrl.host.split(',')
		hostList := (HostDetails[]) hg.map { HostDetails(it, false, this.typeof.pod.log) }
		hostList.last.port = conMgr.connectionUrl.port ?: 27017
		verifyEq(hostList[0].address,	"ds999999-a0.mlab.com")
		verifyEq(hostList[0].port, 		55555)
		verifyEq(hostList[1].address,	"ds999999-a1.mlab.com")
		verifyEq(hostList[1].port, 		44444)
	}	
	
	Void testWriteConcern() {
		// test default
		conMgr := MongoConnUrl(`mongodb://wotever`)
		verifyEq(conMgr.writeConcern, null)

		// write concern
		conMgr = MongoConnUrl(`mongodb://wotever?w=-1`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", -1))
		
		conMgr = MongoConnUrl(`mongodb://wotever?w=0`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 0))
		
		conMgr = MongoConnUrl(`mongodb://wotever?w=1`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 1))

		// write timeout
		conMgr = MongoConnUrl(`mongodb://wotever?wtimeoutMS=2000`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("wtimeout", 2000))
		
		// journal
		conMgr = MongoConnUrl(`mongodb://wotever?journal=true`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("j", true))
		
		// all options
		conMgr = MongoConnUrl(`mongodb://wotever?w=3&wtimeoutMS=1234&journal=true`)
		verifyEq(conMgr.writeConcern, Str:Obj?[:] { it.ordered=true }.add("w", 3).add("wtimeout", 1234).add("j", true))

		verify(logs.isEmpty)
	}
}
