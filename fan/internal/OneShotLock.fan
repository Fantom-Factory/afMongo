
internal class OneShotLock {
	
	public 	Bool	locked
	private Str 	because
	
	new make(Str because) {
		this.because = because
	}
	
	Void lock() {
		check	// you can't lock twice!
		locked = true
	}
	
	public Void check() {
		if (locked)
			throw MongoErr(ErrMsgs.oneShotLock_violation(because))
	}
	
	override Str toStr() {
		(locked ? "" : "(un)") + "locked"
	}
}
