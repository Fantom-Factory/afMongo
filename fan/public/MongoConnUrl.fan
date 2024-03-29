
** Parses a Mongo Connection URL into known options.
** 
** If user credentials are supplied, they are used as default authentication for each connection.
** 
** The following URL options are supported:
** 
**  - 'ssl'
**  - 'tls'
** 
**  - 'connectTimeoutMS'
**  - 'socketTimeoutMS'
** 
**  - 'compressors'
**  - 'zlibCompressionLevel'
** 
**  - 'minPoolSize'
**  - 'maxPoolSize'
**  - 'waitQueueTimeoutMS'
**  - 'maxIdleTimeMS'
** 
**  - 'w'
**  - 'wtimeoutMS'
**  - 'journal'
** 
**  - 'authSource'
**  - 'authMechanism'
**  - 'authMechanismProperties'
** 
**  - 'appName'
**  - 'retryWrites'
**  - 'retryReads'
** 
**  - 'disableTxns' *(unofficial)*
** 
** URL examples:
**  - 'mongodb://username:password@example1.com/database?maxPoolSize=50'
**  - 'mongodb://example2.com?minPoolSize=10&maxPoolSize=50&ssl=true'
** 
** See `https://www.mongodb.com/docs/manual/reference/connection-string/`.
const class MongoConnUrl {
	private const Log	log		:= MongoConnUrl#.pod.log
	
	** The original URL this class was initialised with.
	** May contain authentication details.
	** 
	**   mongodb://username:password@example1.com/puppies?maxPoolSize=50
	const Uri connectionUrl
	
	** The default database name - taken from the path.
	** 
	**   mongodb://example1.com/<database>
	const Str? dbName

	** An "unofficial" option that can be useful for development.
	** 
	** Transactions CANNOT be run against standalone MongoDB servers (MongoDB raises errors).
	** 
	** This option switches transactions off, and instead executes the txnFn outside of a transaction.
	** This means the same app code may be used in both standalone and clustered environments.
	** 
	**   mongodb://example.com/puppies?disableTxns=true
	** 
	** This option exists because MongoDB does NOT provide a sure-fire way of identifying standalone instances.
	** Plus, automatically disabling txns could be dangerous and give the user a false sense of security. 
	const Bool disableTxns
	
	
	
	// ---- TLS Options ----

	** Specifies a TLS / SSL connection. Set to 'true' for Mongo Atlas databases.
	** 
	** Defaults to 'false'. 
	** 
	**   mongodb://example.com/puppies?tls=true
	**   mongodb://example.com/puppies?ssl=true
	const Bool tls := false
	
	
	
	// ---- Timeout Options ----

	** The amount of time to attempt a socket connection before timing out.
	** If 'null' (the default) then a system timeout is used.
	** 
	**   mongodb://example.com/puppies?connectTimeoutMS=25000
	** 
	** Equates to `inet::SocketOptions.connectTimeout`.
	const Duration? connectTimeout
	
	** The amount of time to attempt a send or receive on a socket before timing out.
	** 'null' (the default) indicates an infinite timeout.
	** 
	**   mongodb://example.com/puppies?socketTimeoutMS=25000
	** 
	** Equates to `inet::SocketOptions.receiveTimeout`.
	const Duration? socketTimeout	
	
	
	
	// ---- Compression Options ----
	
	** A list of compressors, as understood by this driver and presented to the MongoDB server.
	** Any options supplied to the MongoURL and not understood by this driver will **not** be present in this list.
	** 
	**   mongodb://example.com/puppies?compressors=snappy,zlib
	** 
	** Mongo understands 'snappy, 'zlib', 'zstd', but currently this driver ONLY understands 'zlib'.
	** 
	** This option may be used to disable wire compression, by suppling an empty list.
	** 
	**   mongodb://example.com/puppies?compressors=
	** 
	** If not defined, this defaults to '["zlib"]'.
	const Str[] compressors
	
	** The compression level (0 - 9) to use with zlib (0 = No compression, 1 = Best speed, 9 = Best compression). 
	** 
	** 'null' indicates a default value will be used. 
	** 
	**   mongodb://example.com/puppies?zlibCompressionLevel=8
	const Int? zlibCompressionLevel
	
	
	
	// ---- Connection Pool Options ----
	
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
	** Defaults to 12 seconds.
	** 
	**   mongodb://example.com/puppies?waitQueueTimeoutMS=12000
	const Duration waitQueueTimeout := 12sec	

	** The maximum time a connection can remain idle in the pool before being removed and 
	** closed. (This does NOT override minPoolSize.)
	** 
	** This helps ease connection throttling during bursts of activity.
	** 
	** Defaults to 10sec.
	** 
	**   mongodb://example.com/puppies?maxIdleTimeMS=15000
	const Duration maxIdleTime	:= 10sec

	
	
	// ---- Write Concern Options ----
	
	** The default write concern for all write operations. 
	** Set by specifying the 'w', 'wtimeoutMS' and 'journal' connection string options. 
	** 
	**   mongodb://username:password@example1.com/puppies?w=1&wtimeout=0&j=false
	const [Str:Obj?]? writeConcern	
	

	
	// ---- Authentication Options ----
	
	** The credentials (if any) used to authenticate connections against MongoDB. 
	const MongoCreds? mongoCreds
	
	** The auth mechanisms used for authenticating connections.
	const Str:MongoAuthMech	authMechs		:= [
		"SCRAM-SHA-1"	: MongoAuthScramSha1(),
	]
	
	
	
	// ---- Miscellaneous Configuration ----
	
	** The application name this client identifies itself to the MongoDB server as.
	** Used by MongoDB when logging.
	** 
	**   mongodb://example.com/puppies?appName=WattsApp
	const Str? appName

	** An option to **turn off** retryable reads.
	** (Defaults to 'true').
	** 
	**   mongodb://example.com/puppies?retryReads=false
	const Bool retryReads
	
	** An option to **turn off** retryable writes.
	** (Defaults to 'true').
	** 
	**   mongodb://example.com/puppies?retryWrites=false
	const Bool retryWrites	



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
		maxIdleTimeMs	 		:= mongoUrl.query["maxIdleTimeMS"]?.toInt
		w						:= mongoUrl.query["w"]
		wtimeoutMs		 		:= mongoUrl.query["wtimeoutMS"]?.toInt
		journal			 		:= mongoUrl.query["journal"]?.toBool
		this.tls		 		 =(mongoUrl.query["tls"]?.toBool ?: mongoUrl.query["ssl"]?.toBool) ?: false
		authSource				:= mongoUrl.query["authSource"]?.trimToNull
		authMech				:= mongoUrl.query["authMechanism"]?.trimToNull
		authMechProps			:= mongoUrl.query["authMechanismProperties"]?.trimToNull
		appName					:= mongoUrl.query["appName"]?.trimToNull
		compressors				:= mongoUrl.query["compressors"]?.split(',')?.exclude { it.isEmpty || it.size > 64 } as Str[]
		zlibCompressionLevel	:= mongoUrl.query["zlibCompressionLevel"]?.toInt(10, false)
		this.retryWrites		 = mongoUrl.query["retryWrites"] != "false"
		this.retryReads			 = mongoUrl.query["retryReads"] != "false"
		this.disableTxns		 = mongoUrl.query["disableTxns"] == "true"

		if (minPoolSize < 0)
			throw ArgErr(errMsg_intTooSmall("minPoolSize", "0", minPoolSize, mongoUrl))
		if (maxPoolSize < 1)
			throw ArgErr(errMsg_intTooSmall("maxPoolSize", "1", maxPoolSize, mongoUrl))
		if (minPoolSize > maxPoolSize)
			throw ArgErr(errMsg_badMinMaxConnectionSize(minPoolSize, maxPoolSize, mongoUrl))		
		if (waitQueueTimeoutMs != null && waitQueueTimeoutMs < 0)
			throw ArgErr(errMsg_intTooSmall("waitQueueTimeoutMS", "0", waitQueueTimeoutMs, mongoUrl))
		if (connectTimeoutMs != null && connectTimeoutMs < 0)
			throw ArgErr(errMsg_intTooSmall("connectTimeoutMS", "0", connectTimeoutMs, mongoUrl))
		if (socketTimeoutMs != null && socketTimeoutMs < 0)
			throw ArgErr(errMsg_intTooSmall("socketTimeoutMS", "0", socketTimeoutMs, mongoUrl))
		if (maxIdleTimeMs != null && maxIdleTimeMs < 0)
			throw ArgErr(errMsg_intTooSmall("maxIdleTimeMS", "0", maxIdleTimeMs, mongoUrl))
		if (wtimeoutMs != null && wtimeoutMs < 0)
			throw ArgErr(errMsg_intTooSmall("wtimeoutMS", "0", wtimeoutMs, mongoUrl))
		if (zlibCompressionLevel != null && zlibCompressionLevel < -1)
			throw ArgErr(errMsg_intTooSmall("zlibCompressionLevel", "-1", zlibCompressionLevel, mongoUrl))
		if (zlibCompressionLevel != null && zlibCompressionLevel > 9)
			throw ArgErr(errMsg_intTooLarge("zlibCompressionLevel", "9", zlibCompressionLevel, mongoUrl))

		if (waitQueueTimeoutMs			!= null)	waitQueueTimeout		= 1ms * waitQueueTimeoutMs
		if (connectTimeoutMs			!= null)	connectTimeout			= 1ms * connectTimeoutMs
		if (socketTimeoutMs				!= null)	socketTimeout			= 1ms * socketTimeoutMs
		if (maxIdleTimeMs				!= null)	maxIdleTime				= 1ms * maxIdleTimeMs

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

		this.dbName = mongoUrl.pathOnly.relTo(`/`).encode.trimToNull
		
		if (authMech != null) {
			props	:= Str:Obj?[:]
			props.ordered = true
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
			
		writeConcern := Str:Obj?[:]
		writeConcern.ordered = true
		if (w != null)
			writeConcern["w"] = Int.fromStr(w, 10, false) != null ? w.toInt : w
		if (wtimeoutMs != null)
			writeConcern["wtimeout"] = wtimeoutMs
		if (journal != null)
			writeConcern["j"] = journal
		if (writeConcern.size > 0)
			this.writeConcern = writeConcern

		if (appName != null) {
			// appName cannot exceed 128 bytes
			// https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst#limitations
			// I know this check is for chars but I'm guessing the reasoning is to just prevent inappropriate hacking attempts
			if (appName.size > 128)
				appName 	= appName[0..<128]
			this.appName	= appName
		}
		
		validCompressors := Str["zlib"]
		this.compressors = compressors?.findAll { validCompressors.contains(it) } ?: validCompressors
		if (zlibCompressionLevel == -1)
			zlibCompressionLevel = null
		this.zlibCompressionLevel	= zlibCompressionLevel
		
		query := mongoUrl.query.rw
		query.remove("minPoolSize")
		query.remove("maxPoolSize")
		query.remove("waitQueueTimeoutMS")
		query.remove("connectTimeoutMS")
		query.remove("socketTimeoutMS")
		query.remove("maxIdleTimeMS")
		query.remove("w")
		query.remove("wtimeoutMS")
		query.remove("journal")
		query.remove("ssl")
		query.remove("tls")
		query.remove("authSource")
		query.remove("authMechanism")
		query.remove("authMechanismProperties")
		query.remove("appName")
		query.remove("compressors")
		query.remove("zlibCompressionLevel")
		query.remove("retryWrites")
		query.remove("retryReads")
		query.remove("disableTxns")
		query.each |val, key| {
			log.warn("Unknown option in Mongo connection URL: ${key}=${val}")
		}
	}

	private static Str errMsg_intTooSmall(Str what, Str min, Int val, Uri mongoUrl) {
		"$what must be >= $min, val=$val, uri=$mongoUrl"
	}

	private static Str errMsg_intTooLarge(Str what, Str min, Int val, Uri mongoUrl) {
		"$what must be <= $min, val=$val, uri=$mongoUrl"
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
