using concurrent::AtomicRef
using afConcurrent::SynchronizedState
using inet::IpAddr
using inet::TcpSocket

class HuntThePrimary {
	private const Log	log				:= HuntThePrimary#.pod.log
	private const Uri	connectionUrl
	private const Bool	ssl
	
	new make(Uri connectionUrl, Bool ssl) {
		this.connectionUrl	= connectionUrl
		this.ssl			= ssl
	}
	
	** (Advanced)
	** Searches the replica set for the Master node and instructs all new connections to connect to it.
	** Throws 'MongoErr' if a primary can not be found. 
	** 
	** This method should be followed with a call to 'emptyPool()'.  
	Uri huntThePrimary() {
		hg		:= connectionUrl.host.split(',')
		hostList := (HostDetails[]) hg.map { HostDetails(it, ssl) }
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
					hosts[hd.primary] = HostDetails(hd.primary, ssl)
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
						hosts[it] = HostDetails(it, ssl)
				}
			}

			// loop through them all
			primary = hosts.find { !it.contacted && it.populate.isPrimary }
		}

		// Bugger!
		if (primary == null)
			throw MongoErr(MongoErrMsgs.connectionManager_couldNotFindPrimary(connectionUrl))

		primaryAddress	:= primary.address
		primaryPort		:= primary.port
		
		// remove user credentials and other crud from the url
		mongoUrl := `mongodb://${primaryAddress}:${primaryPort}`
		if (connectionUrl.pathStr.trimToNull != null)
			mongoUrl = mongoUrl.plusSlash.plusName(connectionUrl.path.first) 
		

//		mongoUrlRef.val = mongoUrl
//
//		// set our connection factory
//		connectionState.sync |ConnectionManagerPoolState state| {
//			state.connectionFactory = |->Connection| {
//				socket := ssl ? TcpSocket.makeTls : TcpSocket.make
//				socket.options.connectTimeout = connectTimeout
//				socket.options.receiveTimeout = socketTimeout
//				return TcpConnection(socket).connect(IpAddr(primaryAddress), primaryPort) {
//					it.mongoUrl = mongoUrlRef.val
//				}
//			} 
//		}

		log.info("Found a new Master at ${mongoUrl}")
		
		return mongoUrl
	}

}

internal class HostDetails {
	static const Log	log	:= HostDetails#.pod.log
	Str		address
	Int		port
	Bool	ssl
	Bool	contacted
	Bool	isPrimary
	Bool	isSecondary
	Str[]	hosts	:= Obj#.emptyList
	Str?	primary
	
	new make(Str addr, Bool ssl) {
		uri	:= `//${addr}`
		this.address = uri.host ?: "127.0.0.1"
		this.port	 = uri.port ?: 27017
		this.ssl	 = ssl
	}
	
	This populate() {
		contacted = true
		
		connection := TcpConnection(ssl)
		try {
			connection.connect(IpAddr(address), port)
			mongUrl	:= `mongodb://${address}:${port}`
			conMgr	:= ConnectionManagerLocal(connection, ssl ? mongUrl.plusQuery(["ssl":"true"]) : mongUrl)
			details := Database(conMgr, "admin").runCmd(["ismaster":1])
		
			isPrimary 	= details["ismaster"]  == true			// '== true' to avoid NPEs if key doesn't exist
			isSecondary	= details["secondary"] == true			// '== true' to avoid NPEs if key doesn't exist in standalone instances  
			primary		= details["primary"]					// standalone instances don't have primary information
			hosts		= details["hosts"] ?: Obj#.emptyList	// standalone instances don't have hosts information
			
		} catch (Err err) {
			// if a replica is down, simply log it and move onto the next one!
			log.warn("Could not connect to Host ${address}:${port} :: ${err.typeof.name} - ${err.msg}")

		} finally connection.close
		
		return this
	}
	
	Str host() { "${address}:${port}" }

	override Str toStr() { host }
}
