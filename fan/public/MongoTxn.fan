
** The context a Mongo transaction runs in.
mixin MongoTxn {
	
	static const Int statusNone			:= 0
	static const Int statusStarting		:= 1
	static const Int statusInProgress	:= 2
	static const Int statusCommitted	:= 3
	static const Int statusAborted		:= 4
	
	abstract Int status
	
	abstract Int txnNum()
	
	abstract Void abort()
	
	abstract Void commit()
}

internal class MongoTxnImpl : MongoTxn {
	
	private	 const	MongoSess	sess
	private			[Str:Obj?]?	recoveryToken
	override const	Int			txnNum
	
	** Allows transactions to be tracked across multiple mongos. 
	
	override Int status {
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
	
	new make(MongoSess sess, Int txnNum) {
		this.sess	= sess
		this.txnNum	= txnNum
	}
	
	override Void abort() {
		
	}
	
	override Void commit() {
		
	}
}