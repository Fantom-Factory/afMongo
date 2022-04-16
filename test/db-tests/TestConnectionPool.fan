using concurrent::Actor
using concurrent::ActorPool
using afConcurrent::Synchronized
using afConcurrent::AtomicList

internal class TestConnectionPool : MongoDbTest {
	static	const AtomicList logs		:= AtomicList()
			const Unsafe handlerRef		:= Unsafe(|LogRec rec| { logs.add(rec) })
				  |LogRec rec| handler() { handlerRef.val }

	MongoConnMgrPool? conMgr
	
	override Void setup() {
		Pod.of(this).log.level = LogLevel.warn
		conMgr = MongoConnMgrPool(`mongodb://localhost:27017?minPoolSize=5`)
		mc = MongoClient(conMgr)

		logs.clear
		Log.addHandler(handler)
		Pod.of(this).log.level = LogLevel.info
	}

	override Void teardown() {
		Log.removeHandler(handler)
		super.teardown
	}
	
	Void testReHuntThePrimary() {
		conMgr := conMgr
		f := Synchronized(ActorPool()).async |->| {
			con := null as MongoTcpConn
			conMgr.leaseConn |MongoTcpConn c| {
				con = c
				Actor.sleep(200ms)
			}
			if (!con.forceCloseOnCheckIn)
				throw Err("Connection not force closed")
		}
		
		Actor.sleep(10ms)
		
		verifyMongoErrMsg("==< MongoDB says: not master >==") |->| {
			conMgr.leaseConn {
				throw Err("==< MongoDB says: not master >==")
			}
		}

		// give the async failOver() time to complete
		Actor.sleep(10ms)
		
		verifyEq((logs.first as LogRec).msg, "Found a new Master at mongodb://localhost:27017")
		verifyEq(conMgr.mongoConnUrl.minPoolSize, 5)
		verifyEq(conMgr.noOfConnectionsInUse, 1)
		verifyEq(conMgr.noOfConnectionsInPool, 6)
		
		f.get
		
		verifyEq(conMgr.noOfConnectionsInUse, 0)
		verifyEq(conMgr.noOfConnectionsInPool, 5)
	}
	
}
