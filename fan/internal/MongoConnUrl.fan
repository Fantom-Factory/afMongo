
** Parses a Mongo Connection URL.
** If user credentials are supplied, they are used as default authentication for each connection.
** 
** The following URL options are supported:
**  - 'minPoolSize'
**  - 'maxPoolSize'
**  - 'waitQueueTimeoutMS'
**  - 'connectTimeoutMS'
**  - 'socketTimeoutMS'
**  - 'w'
**  - 'wtimeoutMS'
**  - 'journal'
**  - 'ssl'
**  - 'tls'
**  - 'authSource'
**  - 'authMechanism'
**  - 'authMechanismProperties'
** 
** URL examples:
**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
** 
** See `https://www.mongodb.com/docs/manual/reference/connection-string/`.
@NoDoc	// advanced use only
const class MongoConnUrl {
	private const Log	log		:= MongoConnUrl#.pod.log
	
	** The original URL this class was initialised with.
	** May contain authentication details.
	** 
	**   mongodb://username:password@example1.com/puppies?maxPoolSize=50
	const Uri connectionUrl

	** The default write concern for all write operations. 
	** Set by specifying the 'w', 'wtimeoutMS' and 'journal' connection string options. 
	** 
	**   mongodb://username:password@example1.com/puppies?w=1&wtimeout=0&j=false
	const [Str:Obj?]? writeConcern
	
	** The minimum number of database connections the pool should keep open.
	** 
	** Defaults to 1.
	** 
	**   mongodb://example.com/puppies?minPoolSize=50
	const Int minPoolSize	:= 1

	** The maximum number of database connections the pool is allowed to open.
	** This is the maximum number of concurrent users you expect your application to have.
	** 
	** Defaults to 10.
	** 
	**   mongodb://example.com/puppies?maxPoolSize=10
	const Int maxPoolSize	:= 10
	
	** The maximum time a thread can wait for a connection to become available.
	** 
	** Defaults to 15 seconds.
	** 
	**   mongodb://example.com/puppies?waitQueueTimeoutMS=10
	const Duration waitQueueTimeout := 15sec

	** The amount of time to attempt a connection before timing out.
	** If 'null' (the default) then a system timeout is used.
	** 
	**   mongodb://example.com/puppies?connectTimeoutMS=2500
	** 
	** Equates to `inet::SocketOptions.connectTimeout`.
	const Duration? connectTimeout
	
	** The amount of time to attempt a send or receive on a socket before timing out.
	** 'null' (the default) indicates an infinite timeout.
	** 
	**   mongodb://example.com/puppies?socketTimeoutMS=2500
	** 
	** Equates to `inet::SocketOptions.receiveTimeout`.
	const Duration? socketTimeout

	** Specifies a TLS / SSL connection. Set to 'true' for Atlas databases.
	** 
	** Defaults to 'false'. 
	** 
	**   mongodb://example.com/puppies?tls=true
	**   mongodb://example.com/puppies?ssl=true
	const Bool tls := false
	
	** The credentials (if any) used to authenticate connections against MongoDB. 
	const MongoCreds? mongoCreds
	
	** The auth mechanisms used for authenticating connections.
	const Str:MongoAuthMech	authMechs		:= [
		"SCRAM-SHA-1"	: MongoAuthScramSha1(),
	]
	
	** Parses a Mongo Connection URL.
	new fromUrl(Uri connectionUrl) {
		if (connectionUrl.scheme != "mongodb")
			throw ArgErr("Mongo connection URIs must start with the scheme 'mongodb://' - ${connectionUrl}")

		mongoUrl				:= connectionUrl
		this.connectionUrl		 = connectionUrl
		this.minPoolSize 		 = mongoUrl.query["minPoolSize"]?.toInt ?: minPoolSize
		this.maxPoolSize 		 = mongoUrl.query["maxPoolSize"]?.toInt ?: maxPoolSize
		waitQueueTimeoutMs		:= mongoUrl.query["waitQueueTimeoutMS"]?.toInt
		connectTimeoutMs		:= mongoUrl.query["connectTimeoutMS"]?.toInt
		socketTimeoutMs 		:= mongoUrl.query["socketTimeoutMS"]?.toInt
		w						:= mongoUrl.query["w"]
		wtimeoutMs		 		:= mongoUrl.query["wtimeoutMS"]?.toInt
		journal			 		:= mongoUrl.query["journal"]?.toBool
		this.tls		 		 =(mongoUrl.query["tls"]?.toBool ?: mongoUrl.query["ssl"]?.toBool) ?: false
		authSource				:= mongoUrl.query["authSource"]?.trimToNull
		authMech				:= mongoUrl.query["authMechanism"]?.trimToNull
		authMechProps			:= mongoUrl.query["authMechanismProperties"]?.trimToNull

		if (minPoolSize < 0)
			throw ArgErr(errMsg_badInt("minPoolSize", "zero", minPoolSize, mongoUrl))
		if (maxPoolSize < 1)
			throw ArgErr(errMsg_badInt("maxPoolSize", "one", maxPoolSize, mongoUrl))
		if (minPoolSize > maxPoolSize)
			throw ArgErr(errMsg_badMinMaxConnectionSize(minPoolSize, maxPoolSize, mongoUrl))		
		if (waitQueueTimeoutMs != null && waitQueueTimeoutMs < 0)
			throw ArgErr(errMsg_badInt("waitQueueTimeoutMS", "zero", waitQueueTimeoutMs, mongoUrl))
		if (connectTimeoutMs != null && connectTimeoutMs < 0)
			throw ArgErr(errMsg_badInt("connectTimeoutMS", "zero", connectTimeoutMs, mongoUrl))
		if (socketTimeoutMs != null && socketTimeoutMs < 0)
			throw ArgErr(errMsg_badInt("socketTimeoutMS", "zero", socketTimeoutMs, mongoUrl))
		if (wtimeoutMs != null && wtimeoutMs < 0)
			throw ArgErr(errMsg_badInt("wtimeoutMS", "zero", wtimeoutMs, mongoUrl))

		if (waitQueueTimeoutMs != null)
			waitQueueTimeout = (waitQueueTimeoutMs * 1_000_000).toDuration
		if (connectTimeoutMs != null)
			connectTimeout = (connectTimeoutMs * 1_000_000).toDuration
		if (socketTimeoutMs != null)
			socketTimeout = (socketTimeoutMs * 1_000_000).toDuration

		// authSource trumps defaultauthdb 
		database := authSource ?: mongoUrl.pathStr.trimToNull
		username := mongoUrl.userInfo?.split(':')?.getSafe(0)?.trimToNull
		password := mongoUrl.userInfo?.split(':')?.getSafe(1)?.trimToNull
		
		if ((username == null).xor(password == null))
			throw ArgErr(errMsg_badUsernamePasswordCombo(username, password, mongoUrl))

		if (database != null && database.startsWith("/"))
			database = database[1..-1].trimToNull
		if (username != null && password != null && database == null)
			database = "admin"
		if (username == null && password == null)	// a default database has no meaning without credentials
			database = null
		
		if (authMech != null) {
			props	:= Str:Obj?[:] { it.ordered = true }
			authMechProps?.split(',')?.each |pair| {
				if (pair.size > 0) {
					key := pair
					val := null
					idx := pair.index(":")
					if (idx != null) {
						key = pair[0..<idx]
						val = pair[idx+1..-1]
					}
					props[key] = val
				}
			}
			this.mongoCreds	= MongoCreds {
				it.mechanism	= authMech
				it.source		= database
				it.username		= username
				it.password		= password
				it.props		= props
			}
		}
		
		// set some default creds
		if (this.mongoCreds == null && username != null && password != null)
			this.mongoCreds	= MongoCreds {
				it.mechanism	= "SCRAM-SHA-1"
				it.source		= database
				it.username		= username
				it.password		= password
			}
			
		writeConcern := Str:Obj?[:] { it.ordered = true }
		if (w != null)
			writeConcern["w"] = Int.fromStr(w, 10, false) != null ? w.toInt : w
		if (wtimeoutMs != null)
			writeConcern["wtimeout"] = wtimeoutMs
		if (journal != null)
			writeConcern["j"] = journal
		if (writeConcern.size > 0)
			this.writeConcern = writeConcern

		query := mongoUrl.query.rw
		query.remove("minPoolSize")
		query.remove("maxPoolSize")
		query.remove("waitQueueTimeoutMS")
		query.remove("connectTimeoutMS")
		query.remove("socketTimeoutMS")
		query.remove("w")
		query.remove("wtimeoutMS")
		query.remove("journal")
		query.remove("ssl")
		query.remove("tls")
		query.remove("authSource")
		query.remove("authMechanism")
		query.remove("authMechanismProperties")
		query.each |val, key| {
			log.warn("Unknown option in Mongo connection URL: ${key}=${val}")
		}
	}

	private static Str errMsg_badInt(Str what, Str min, Int val, Uri mongoUrl) {
		"$what must be greater than $min! val=$val, uri=$mongoUrl"
	}
	
	private static Str errMsg_badMinMaxConnectionSize(Int min, Int max, Uri mongoUrl) {
		"Minimum number of connections must not be greater than the maximum! min=$min, max=$max, url=$mongoUrl"
	}
		
	private static Str errMsg_unknownAuthMechanism(Str mechanism, Str[] supportedMechanisms) {
		"Unknown authentication mechanism '${mechanism}', only the following are currently supported: " + supportedMechanisms.join(", ")
	}
	
	private static Str errMsg_badUsernamePasswordCombo(Str? username, Str? password, Uri mongoUrl) {
		"Either both the username and password should be provided, or neither. username=$username, password=$password, url=$mongoUrl"
	}	
}
