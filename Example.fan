    using afBson
    using afMongo
    using concurrent

    class Example {

        Void main() {
            mongoClient := MongoClient(ActorPool(), `mongodb://localhost:27017`)
            collection  := mongoClient.db("friends").collection("birds")

            documentIn  := [
                "_id"   : ObjectId(),
                "name"  : "Emma",
                "score" : 9
            ]
            collection.insert(documentIn)

            echo( collection.findAll.first )

            mongoClient.shutdown
        }
    }
