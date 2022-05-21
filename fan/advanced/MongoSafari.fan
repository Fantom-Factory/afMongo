
internal class MongoSafari {
	private const Log	log
	private const Uri	connectionUrl
	private const Bool	ssl
	private const Str?	appName
	
	new make(MongoConnUrl mongoConnUrl, Log log) {
		this.connectionUrl	= mongoConnUrl.connectionUrl
		this.ssl			= mongoConnUrl.tls
		this.appName		= mongoConnUrl.appName
		this.log			= log
	}
	
	** (Advanced)
	** Searches the replica set for the Master node and instructs all new connections to connect to it.
	** Throws an 'Err' if a primary can not be found. 
	** 
	** This method should be followed with a call to 'emptyPool()'.  
	MongoHostDetails huntThePrimary() {
		hg		:= connectionUrl.host.split(',')
		hostList := (Mongo4x4[]) hg.map { Mongo4x4(it, ssl, appName, log) }
		hostList.last.port = connectionUrl.port ?: 27017
		hosts	:= Str:Mongo4x4[:] { it.ordered=true }.addList(hostList) { it.host }
		
		// default to the first host
		primary	:= (Mongo4x4?) null
		
		// let's play hunt the primary! Always check, even if only 1 host is supplied, it may still 
		// be part of a replica set
		// first, check the list of supplied hosts
		primary = hostList.eachWhile |hd->Mongo4x4?| {
			// Is it? Is it!?
			if (hd.populate.isPrimary)
				return hd

			// now lets contact what it thinks is the primary, to double check
			// assume if it's been contacted, it's not the primary - cos we would have returned it already
			if (hd.primary != null && hosts[hd.primary]?.contacted != true) {
				if (hosts[hd.primary] == null) 
					hosts[hd.primary] = Mongo4x4(hd.primary, ssl, appName, log)
				if (hosts[hd.primary].populate.isPrimary)
					return hosts[hd.primary]
			}

			// keep looking!
			return null
		}

		// the above should have flushed out the primary, but if not, check *all* the returned hosts
		if (primary == null) {
			// add all the hosts to our map
			hostList.each |hd| {
				hd.hosts.each {
					if (hosts[it] == null)
						hosts[it] = Mongo4x4(it, ssl, appName, log)
				}
			}

			// loop through them all
			primary = hosts.find { !it.contacted && it.populate.isPrimary }
		}

		// Bugger!
		if (primary == null)
			throw Err("Could not find the primary node with RelicaSet connection URL ${connectionUrl}")

		log.info("Found a new Master at ${primary.mongoUrl}")
		
		return primary.hostDetails
	}
}

internal class Mongo4x4 {
	Str		address
	Int		port
	Bool	ssl
	Str?	appName
//	Str[]	compression
	Log		log

	MongoHostDetails?	hostDetails
	
	new make(Str addr, Bool ssl, Str? appName, Log log) {
		uri	:= `//${addr}`
		this.address	= uri.host ?: "127.0.0.1"
		this.port		= uri.port ?: 27017
		this.ssl		= ssl
		this.appName	= appName
		this.log		= log
	}
	
	This populate() {
		connection	:= MongoTcpConn(ssl, log)
		client		:= Str:Obj?[
			"driver"			: Str:Obj?[
				"name"			: typeof.pod.name,
				"version"		: typeof.pod.version.toStr,
			],
			"os"				: Str:Obj?[
				"type"			: Env.cur.os,
				"architecture"	: Env.cur.arch,
				"platform"		: Env.cur.runtime,
			],
		]
		if (appName != null)
			client["application"]	= Str:Obj?["name" : appName]

		try {
//			connection.log.level = LogLevel.debug
			connection.connect(address, port)
			
			// I have a feeling, the "hello" cmd only works via OP_MSG on Mongo v4.4 or later
			// so lets keep it running the legacy "isMaster" until I migrate my prod databases
			details	:= MongoOp(connection).runCommand("admin", map.add("isMaster", 1).add("client", client), false)
			if (details["ok"] != 1f)
				details		= MongoOp(connection).runCommand("admin", map.add("hello", 1).add("client", client), false)
		
			this.hostDetails = MongoHostDetails(mongoUrl, details)
			
		} catch (Err err)
			// if a replica is down, simply log it and move onto the next one!
			log.warn("Could not connect to Host ${address}:${port} :: ${err.typeof.name} - ${err.msg}", err)

		return this
	}

	Str[]	hosts()			{ hostDetails.hosts }
	Str?	primary()		{ hostDetails.primary }
	Bool	isPrimary()		{ contacted && hostDetails.isValid && hostDetails.isPrimary }
	Bool	contacted()		{ this.hostDetails != null }
	Str		host()			{ "${address}:${port}" }
	Uri		mongoUrl()		{ `mongodb://${address}:${port}` }

	override Str toStr() { host }

	private [Str:Obj?] map() { Str:Obj?[:] { ordered = true } }
}

internal const class MongoHostDetails {

	const Uri	mongoUrl
	const Int	maxWireVer
	const Str?	primary
	const Bool	isPrimary
	const Bool	isSecondary
	const Str[]	hosts
	const Str[]	compression
	
	new make(Uri mongoUrl, Str:Obj? details) {
		this.mongoUrl		= mongoUrl
		this.maxWireVer		= details["maxWireVersion"]
		// "ismaster" for "isMaster" cmds, and "isWritablePrimary" for "hello" cmds.
		this.isPrimary 		= details["ismaster"]  == true || details["isWritablePrimary"] == true
		this.isSecondary	= details["secondary"] == true
		this.primary		= details["primary"]					// standalone instances don't have primary information
		this.hosts			= details["hosts"] 			?: Str#.emptyList	// standalone instances don't have hosts information
		this.compression	= details["compression"]	?: Str#.emptyList	
	}
	
	Bool isValid() {
		// 6 supports OP_MSGs == MongoDB 3.6
		// https://github.com/mongodb/specifications/blob/master/source/wireversion-featurelist.rst
		maxWireVer >= 6
	}
}
