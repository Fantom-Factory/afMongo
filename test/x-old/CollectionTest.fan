////////////////////////////////////////////////////////////////////////////////
//
//  Copyright 2010 Liam Staskawicz
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////


using afBson

**
**  CollectionTest
**
class CollectionTest : OldMongoTest
{
  
  CollectionO c := db["tester"]
  
  override Void setup()
  {
    c.drop
  }
  
  override Void teardown()
  {
    // db.drop
  }
  
  Void testValidNames()
  {
    verifyErr(ArgErr#) { co := db[""] }
    verifyErr(ArgErr#) { co := db[".bad"] }
    verifyErr(ArgErr#) { co := db["notgood."] }
  }
  
  Void testInsert()
  {
    c.insert(["foofoo":567])
    verify(c.findOne()["foofoo"] == 567)
    verifyEq(1, c.find().count())
    c.insertDocs([["t":1], ["g":2], ["h":3]])
    verifyEq(4, c.find().count())
    c.drop
    verifyEq(0, c.find().count())
  }
  
  Void NOtestLotsOfInserts()
  {
    c := db["actortest"]
    // a := Actor(ActorPool()) |Int i| {
    //   c.insert(["testincrement": i])
    // }
    
    c.insert(["warmup":true]) // make sure the collection has been created, etc.
    
    runs := 25
    runlens := Float[,]
    runs.times |run| {
      inserts := 2	//10000
      start := Duration.now
      inserts.times |i| {
        c.insert(["testincrement": i])
        // f := a.send(i)
        // if(i == (inserts - 1))
        //   f.get
      }
      elapsed := (Duration.now - start).toMillis().toFloat
      stat := inserts.toFloat/(elapsed/1000f)
      runlens.add(stat)
      echo("run ${run}: ${inserts} inserts - ${elapsed} millis (${stat} insert/sec)")
    }
    sum := runlens.reduce(0f) |Float r, Float v->Float| { return v + r }
    echo("average run time - ${(Float)sum/runlens.size.toFloat}")
    // c.drop
  }
  
  Void testIndex()
  {
    c := db["idxtest"]
    c.drop
    ii := c.indexInfo()
    verifyEq(0, ii.size)
    
    singleidx := c.createIndex([["single":Mongo.DESCENDING]])
    ii = c.indexInfo()
    verifyEq(2, ii.size) // _id is always indexed in addition to what we just added
    verify(ii.containsKey(singleidx))
    verifyEq(singleidx, CollectionO.indexName([["single":Mongo.DESCENDING]]))
    
    doubleidx := c.createIndex([["a":Mongo.DESCENDING], ["b":Mongo.ASCENDING]])
    ii = c.indexInfo()
    verifyEq(3, ii.size)
    verify(ii.keys.containsAll([singleidx, doubleidx]))
    verify((ii[doubleidx] as Map).keys.containsAll(["a", "b"]))
    
    uniqueidx := c.createIndex([["uni":Mongo.ASCENDING]], true)
    verifyEq(uniqueidx, CollectionO.indexName([["uni":Mongo.ASCENDING]]))
    ii = c.indexInfo()
    verifyEq(4, ii.size)
    verify(ii.keys.containsAll([singleidx, doubleidx, uniqueidx]))
    
    c.createIndex([["single":Mongo.DESCENDING]]) // duplicate of first index
    ii = c.indexInfo()
    verifyEq(4, ii.size) // shouldn't create another one
    
    c.dropIndex(singleidx)
    ii = c.indexInfo()
    verifyEq(3, ii.size)
    
    c.dropAllIndexes()
    ii = c.indexInfo()
    verifyEq(1, ii.size) // leaves the index for _id around
    
    c.drop
  }
  
  // Void testValidate()
  // {
  //   c := db["testValidate"]
  //   c.validate() // just run it, confirm no Errs
  //   c.drop
  // }
  
  Void testMapReduce()
  {
    c := db["mapreducetest"]
    c.drop
    
    c.insert(["user_id": 1])
    c.insert(["user_id": 2])
    
    map := "function() { emit(this.user_id, 1); }"
    red := "function(k,vals) { return 1; }"
    res := c.mapReduce(map, red)
    mrcoll := db[res["result"]]
    verifyNotNull(mrcoll.findOne(["_id": 1]))
    verifyNotNull(mrcoll.findOne(["_id": 2]))
    
    c.drop
    c.insert(["user_id": 1])
    c.insert(["user_id": 2])
    c.insert(["user_id": 3])

    map = "function() { emit(this.user_id, 1); }"
    red = "function(k,vals) { return 1; }"
    res = c.mapReduce(map, red, "reduceOut", ["query": ["user_id": ["\$gt": 1]]])
    mrcoll = db[res["result"]]
    verifyEq(2, mrcoll.find.count)
    verifyNull(mrcoll.findOne(["_id": 1]))
    verifyNotNull(mrcoll.findOne(["_id": 2]))
    verifyNotNull(mrcoll.findOne(["_id": 3]))
    
    c.drop
  }
  
  Void testDistinct()
  {
    c := db["distincttest"]
    c.drop
    
    c.insertDocs([["a": 0, "b": ["c": "a"]],
                   ["a": 1, "b": ["c": "b"]],
                   ["a": 1, "b": ["c": "c"]],
                   ["a": 2, "b": ["c": "a"]],
                   ["a": 3],
                   ["a": 3]])
    
    expected := [2, 3]
    c.distinct("a", ["a": ["\$gt": 1]]).sort.each |Int i, idx| {
      verifyEq(expected[idx], i)
    }
    expected2 := ["a", "b"]
    c.distinct("b.c", ["b.c": ["\$ne": "c"]]).sort.each |Str s, idx| {
      verifyEq(expected2[idx], s)
    }
    
    c.drop
  }
  
  Void testGroup()
  {
    c := db["grouptest"]
    c.drop
    
    // save items to the coll
    c.save(["x":"a"])
    c.save(["x":"a"])
    c.save(["x":"a"])
    c.save(["x":"b"])
    initial := ["count": 0.0f]
    f := "function (obj, prev) { prev.count += inc_value; }"
    
    g := c.group(["x"], initial, Code(f, ["inc_value":1]))
    verifyEq(3f, g[0]["count"])
    
    g2 := c.group(["x"], initial, Code(f, ["inc_value":0.5f]))
    verifyEq(1.5f, g2[0]["count"])
    
    // with finalize
    fin := "function(doc) {doc.f = doc.count + 200; }"
    g3 := c.group(Str[,], initial, Code(f, ["inc_value":1]), [:], fin)
    verifyEq(204f, g3[0]["f"])
    
    c.drop
  }

}

