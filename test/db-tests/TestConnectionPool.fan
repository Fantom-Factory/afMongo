using concurrent::Actor
using concurrent::ActorPool
using afConcurrent::Synchronized
using afConcurrent::AtomicList

internal class TestConnectionPool : MongoDbTest {
	static	const AtomicList logs		:= AtomicList()
			const Unsafe handlerRef		:= Unsafe(|LogRec rec| { logs.add(rec) })
				  |LogRec rec| handler() { handlerRef.val }

	MongoConnMgr? connMgr
	
	override Void setup() {
		connMgr = MongoConnMgr(`mongodb://localhost:27017?minPoolSize=5`)
		mc = MongoClient(connMgr)

		logs.clear
		Log.addHandler(handler)
		connMgr.log.level = LogLevel.info
	}

	override Void teardown() {
		Log.removeHandler(handler)
		super.teardown
	}
	
	Void testReHuntThePrimary() {
		connMgr := connMgr
		f := Synchronized(ActorPool()).async |->| {
			con := null as MongoTcpConn
			connMgr.leaseConn |MongoTcpConn c| {
				con = c
				Actor.sleep(200ms)
			}
			if (!con._forceCloseOnCheckIn)
				throw Err("Connection not force closed")
		}
		
		Actor.sleep(10ms)
		
		// IOErrs should force a new Safari
		verifyErrMsg(IOErr#, "==< MongoDB says: not master >==") |->| {
			connMgr.leaseConn {
				throw IOErr("==< MongoDB says: not master >==")
			}
		}

		// give the async failOver() time to complete
		Actor.sleep(10ms)
		
		verifyEq((logs[0] as LogRec)?.msg, "Failing over. Re-scanning network topology for new master...")
		verifyEq((logs[1] as LogRec)?.msg, "Found a new Master at mongodb://localhost:27017 (zlib compression)")
		verifyEq(connMgr.mongoConnUrl.minPoolSize,	5)
		verifyEq(connMgr.props["numConns"],			6)
		verifyEq(connMgr.props["numConnsInUse"],	1)
		
		f.get
		
		verifyEq(connMgr.props["numConns"],			5)
		verifyEq(connMgr.props["numConnsInUse"],	0)
	}
}
