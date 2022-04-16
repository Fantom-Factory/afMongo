
class HuntThePrimary {
	private const Log	log
	private const Uri	connectionUrl
	private const Bool	ssl
	
	new make(Uri connectionUrl, Bool ssl, Log log) {
		this.connectionUrl	= connectionUrl
		this.ssl			= ssl
		this.log			= log
	}
	
	** (Advanced)
	** Searches the replica set for the Master node and instructs all new connections to connect to it.
	** Throws an 'Err' if a primary can not be found. 
	** 
	** This method should be followed with a call to 'emptyPool()'.  
	Uri huntThePrimary() {
		hg		:= connectionUrl.host.split(',')
		hostList := (HostDetails[]) hg.map { HostDetails(it, ssl, log) }
		hostList.last.port = connectionUrl.port ?: 27017
		hosts	:= Str:HostDetails[:] { it.ordered=true }.addList(hostList) { it.host }
		
		// default to the first host
		primary	:= (HostDetails?) null
		
		// let's play hunt the primary! Always check, even if only 1 host is supplied, it may still 
		// be part of a replica set
		// first, check the list of supplied hosts
		primary = hostList.eachWhile |hd->HostDetails?| {
			// Is it? Is it!?
			if (hd.populate.isPrimary)
				return hd

			// now lets contact what it thinks is the primary, to double check
			// assume if it's been contacted, it's not the primary - cos we would have returned it already
			if (hd.primary != null && hosts[hd.primary]?.contacted != true) {
				if (hosts[hd.primary] == null) 
					hosts[hd.primary] = HostDetails(hd.primary, ssl, log)
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
						hosts[it] = HostDetails(it, ssl, log)
				}
			}

			// loop through them all
			primary = hosts.find { !it.contacted && it.populate.isPrimary }
		}

		// Bugger!
		if (primary == null)
			throw Err("Could not find the primary node with RelicaSet connection URL ${connectionUrl}")

		primaryAddress	:= primary.address
		primaryPort		:= primary.port
		
		// remove user credentials and other crud from the url
		mongoUrl		:= `mongodb://${primaryAddress}:${primaryPort}`
		databaseName	:= connectionUrl.path.first?.trimToNull
		if (databaseName != null)
			mongoUrl = mongoUrl.plusSlash.plusName(databaseName) 

		log.info("Found a new Master at ${mongoUrl}")
		
		return mongoUrl
	}

}

internal class HostDetails {
	Str		address
	Int		port
	Bool	ssl
	Log		log

	Bool	contacted
	Bool	isPrimary
	Bool	isSecondary
	Str[]	hosts	:= Obj#.emptyList
	Str?	primary
	
	new make(Str addr, Bool ssl, Log log) {
		uri	:= `//${addr}`
		this.address = uri.host ?: "127.0.0.1"
		this.port	 = uri.port ?: 27017
		this.ssl	 = ssl
		this.log	 = log
	}
	
	This populate() {
		contacted = true
		
		connection	:= TcpConnection(ssl)
		mongUrl		:= `mongodb://${address}:${port}`
		try {
			connection.connect(address, port)
			
			// I have a feeling, the "hello" cmd only works via OP_MSG on Mongo v4.4 or later
			// so lets keep it running the legacy "isMaster" until I migrate my prod databases
			details		:= MongoOp(connection).runCommand("admin", map.add("isMaster", 1), false)
			if (details["ok"] != 1f)
				details	= MongoOp(connection).runCommand("admin", map.add("hello", 1), false)
		
			// "ismaster" for "isMaster" cmds, and "isWritablePrimary" for "hello" cmds.
			isPrimary 	= details["ismaster"]  == true || details["isWritablePrimary"] == true
			isSecondary	= details["secondary"] == true
			primary		= details["primary"]					// standalone instances don't have primary information
			hosts		= details["hosts"] ?: Obj#.emptyList	// standalone instances don't have hosts information
			
		} catch (Err err)
			// if a replica is down, simply log it and move onto the next one!
			log.warn("Could not connect to Host ${address}:${port} :: ${err.typeof.name} - ${err.msg}", err)
		
		return this
	}
	
	Str host() { "${address}:${port}" }

	override Str toStr() { host }

	private [Str:Obj?] map() { Str:Obj?[:] { ordered = true } }
}
