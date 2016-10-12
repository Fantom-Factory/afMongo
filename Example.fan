    using afBson
    using afMongo
    using concurrent
    
    class Example {
        
        Void main() {
            mongoClient := MongoClient(ActorPool(), `mongodb://localhost:27017`)
            collection  := mongoClient.db("friends").collection("birds")
            
            documentIn  := [
                "_id"    : ObjectId(),
                "name"    : "Emma",
                "score"    : 9
            ]
            collection.insert(documentIn)
            
            emma   := collection.findAll.first
            result := PrettyPrinter { it.maxWidth = 20 }.print(emma)
            
            echo("Emma:")
            echo(result)
            
            mongoClient.shutdown
        }
    }