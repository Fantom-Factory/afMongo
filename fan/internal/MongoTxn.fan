using concurrent::Actor

** The context a Mongo transaction runs in.
internal class MongoTxn {
	// nonTxnCmds is just the same as MongoOp.nonSessionCmds
	private static const Str[]		nonTxnCmds		:= "hello isMaster saslStart saslContinue getnonce authenticate".split

	static	const Int statusNone		:= 0
	static	const Int statusStarting	:= 1
	static	const Int statusInProgress	:= 2
	static	const Int statusCommitted	:= 3
	static	const Int statusAborted		:= 4

			const MongoConnMgr	connMgr
			const Int			txnNum
			const MongoSess		sess
				  [Str:Obj?]? 	txnOpts
				  Bool			funcRun

	** Allows transactions to be tracked across multiple mongos. 
	[Str:Obj?]?	recoveryToken
	
	** Represents the MongoDB server transaction state.
	Int status {
		set {
			if (it < statusNone || it > statusAborted)					throw ArgErr("Invalid status: ${it}")
			if (&status == statusNone		&& it == statusCommitted)	throw Err("No transaction started")
			if (&status == statusNone		&& it == statusAborted)		throw Err("No transaction started")
			if (&status == statusStarting	&& it == statusStarting)	throw Err("Transaction already in progress")
			if (&status == statusInProgress	&& it == statusStarting)	throw Err("Transaction already in progress")
			if (&status == statusCommitted	&& it == statusAborted)		throw Err("Cannot call abortTransaction after calling commitTransaction")
			if (&status == statusAborted	&& it == statusCommitted)	throw Err("Cannot call commitTransaction after calling abortTransaction")
			if (&status == statusAborted	&& it == statusAborted)		throw Err("Cannot call abortTransaction twice")
			
			if (it == statusNone || it == statusStarting)
				recoveryToken = null

			&status = it
		}
	}
	
	new make(MongoConnMgr connMgr, MongoSess sess, Int txnNum) {
		this.connMgr	= connMgr
		this.sess		= sess
		this.txnNum		= txnNum
		sess.isDetached	= true		// you're mine now! Muhahahahaha!
	}
	
	Void run([Str:Obj?]? txnOpts, |Obj?| fn) {
		txnOpts = this.txnOpts = txnOpts ?: Str:Obj[:]
		
		if (cur != null)
			throw Err("Mongo Transaction already in progress (txnNum:${cur.txnNum})")
		
		try {
			try {
				Actor.locals["afMongo.txn"] = this
				status	= statusStarting
				fn(this)
				funcRun = true
				
			} catch (IOErr err) {
				
				// let network errors propagate so the whole txn may be retried
				// don't worry about aborting, the txn will timeout on it's own
				throw err
				
			} catch (Err err) {
				
				if (status == statusInProgress) {
					cmd := MongoCmd(connMgr, "admin", "abortTransaction", 1, sess)
					cmd->writeConcern	= txnOpts["writeConcern"] ?: connMgr.mongoConnUrl.writeConcern
					cmd.run(false)
					status = statusAborted
				}

				throw err
			}
			
			if (status == statusInProgress) {
				cmd := MongoCmd(connMgr, "admin", "commitTransaction", 1, sess)
				cmd->writeConcern	= txnOpts["writeConcern"] ?: connMgr.mongoConnUrl.writeConcern
				cmd->maxTimeMS		= txnOpts["maxTimeMS"]		// ->trap so nulls are not added
				cmd.run
				status = statusCommitted
			}

		} finally {
			Actor.locals.remove("afMongo.txn")
			sess.postTxnCheckin
		}
	}
	
	Void prepCmd(Str:Obj? cmd) {
		// make sure failovers and huntThePrimary aren't part of txns!
		if (nonTxnCmds.contains(cmd.keys.first))
			return

		if (cmd.containsKey("readConcern"))
			throw Err("Cannot set read concern after starting a transaction")

		cmd["lsid"]			= sess.sessionId	
		cmd["txnNumber"]	= txnNum
		cmd["autocommit"]	= false

		if (status == statusStarting) {
			cmd["startTransaction"]	= true
			// do NOT inherit *this* readConcen from the client
			if (txnOpts["readConcern"] != null)
				cmd["readConcern"] = txnOpts["readConcern"]
		}
		
		if (recoveryToken != null)
			cmd["recoveryToken"] = recoveryToken
		
		// Only "abortTransaction" and "commitTransaction" are allowed a 'writeConcern' but ...
		// ...it is added automatically when cmds are created - doh!
		// so remove any writeConcern that was added automatically from ConnMgr using same equals '===' 
		//
		// MongoDB says: writeConcern is not allowed within a multi-statement transaction
		// https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#behavior-of-the-writeconcern-field
		if (cmd.containsKey("abortTransaction") == false && cmd.containsKey("commitTransaction") == false)
			if (cmd["writeConcern"] === connMgr.mongoConnUrl.writeConcern)
				cmd.remove("writeConcern")
		
		status = statusInProgress
	}
	
	Void postCmd(Str:Obj? res) {
		if (res.containsKey("recoveryToken"))
			recoveryToken = res["recoveryToken"]
	}
	
	// we *may* introduce these manual methods at a later date
	// but *would* encourage bad code design - so maybe not?
//	abstract Void abort()
//	abstract Void commit()
	
	static MongoTxn? cur() {
		Actor.locals["afMongo.txn"]
	}
}