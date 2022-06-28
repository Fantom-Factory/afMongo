using concurrent::Actor

internal const class MongoBackoff {
	
	// used to test the backoff func
	const |Range->Int|	randomFunc	:= |Range r->Int| { r.random }
	const |Duration| 	sleepFunc	:= |Duration napTime| { Actor.sleep(napTime) }
	
	new make(|This|? f := null) { f?.call(this) }

	
	** Implements a truncated binary exponential backoff algorithm. *Damn, I'm good!*
	** Returns 'null' if the operation timed out.
	** 
	** @see `http://en.wikipedia.org/wiki/Exponential_backoff`
	Obj? backoffFunc(|Duration totalNapTime->Obj?| func, Duration timeout) {
		result			:= null
		c				:= 0
		i				:= 10
		totalNapTime	:= 0ms
		ioErr			:= null
		
		while (result == null && totalNapTime < timeout) {

			// null would be returned if there are no available connections in the pool
			// IOErr would be thrown is we could not connect to the server
			// Note that the re-tryable reads and writes are only applicable AFTER we've obtained a connection
			// i.e. AFTER this method returns successfully
			try result = func.call(totalNapTime)
			catch (IOErr err)
				// save the first err, and keep trying
				ioErr = ioErr ?: err

			if (result == null) {
				if (++c > i) c = i	// truncate the exponentiation ~ 10 secs
				napTime := (randomFunc(0..<2.pow(c)) * 10 * 1000000).toDuration

				// don't over sleep!
				if ((totalNapTime + napTime) > timeout)
					napTime = timeout - totalNapTime 

				sleepFunc(napTime)
				totalNapTime += napTime
				
				// if we're about to quit, lets have 1 more last ditch attempt!
				if (totalNapTime >= timeout)
					result = func.call(totalNapTime)
			}
		}
		
		// if we can't connect to the server after all this time, let people know
		if (result == null && ioErr != null)
			throw ioErr

		return result
	}
}
