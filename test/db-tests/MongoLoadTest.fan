using concurrent::Actor
using concurrent::ActorPool
using concurrent::Future

** As suggested by Matthew Giannini
internal class MongoLoadTest {
	
	Void main(Str[] args) {
		p := ActorPool()
		m := makeClient
		d := m["testA"]
		d.drop
		c := d["foo"]
		1000.times |x| {
			c.insert(Str:Obj?["_id": x, "val": x])
		}
		futures := Future[,]
		101.times |x| {
			echo("Actor $x go!")
			f := Actor(p) |msg->Obj?| {
				1000.times |y| {
					c.update(Str:Obj?["_id":y], Str:Obj?["val": y * 100]) { it->upsert = true }
				}	
				return null
			}.send("go")
			futures.add(f)
		}
		Future.waitForAll(futures)
	}
	
	MongoClient makeClient() {
		MongoClient.makeFromUri(`mongodb://localhost:27017/`)
	}
}
