using afBson
//using afMongo
using concurrent

internal
class Example {
	
	Void main() {
		mongoDbase := MongoClient(ActorPool(), `mongodb://localhost:27017`)
		collection := mongoDbase.db("friends").collection("birds")
		
		documentIn := [
			"_id"	: ObjectId(),
			"name"	: "Emma",
			"age"	: 32
		]
		
		collection.insert(documentIn)
		documentOut := collection.findAll.first
		
		echo(documentOut)
	}
}
