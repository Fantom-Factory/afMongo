using afBson
//using afMongo

internal
class MongoExample {
	
	Void main() {
		mongoUrl	:= `mongodb://localhost:27017`
		mongoClient := MongoClient(mongoUrl)
		collection  := mongoClient.db("friends").collection("birds")
		
		documentIn  := [
			"_id"	: ObjectId(),
			"name"	: "Emma",
			"score"	: 9
		]
		collection.insert(documentIn)
		
		emma		:= collection.find.toList.first
		
		echo("Emma:")
		echo(BsonIO().print(emma, 20))
		
		mongoClient.shutdown
	}
}
