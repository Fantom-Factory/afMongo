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

**
**  CursorTest
**
class CursorTest : OldMongoTest
{
  
  
  Void testLimit()
  {
    coll := db["limtest"]
    coll.drop
    verifyEq(0, coll.find().count)
    i := 0
    10.times { coll.insert(["limmmmm":i++]) }
    verifyEq(10, coll.find().count)
    verifyEq(3, coll.find().limit(3).count)
    
    // test passing in a limit that is less than the number already returned.
    // should have no effect - we should just get all the elements out of the cursor
    curs := coll.find()
    5.times { curs.next }
    curs.limit(3)
    verify(curs.more)
    5.times { curs.next }
    verifyFalse(curs.more)
    
    // test passing it to the cursor ctor
    curs = coll.find([:], ["limit":3])
    verifyEq(3, curs.count)
    3.times { curs.next }
    verifyFalse(curs.more)
    coll.drop
  }
  
  Void testSkip()
  {
    coll := db["skiptest"]
    coll.drop
    verifyEq(0, coll.find().count)
    
    i := 0
    20.times { coll.insert(["skipppppppppppppppp":i++]) }
    verifyEq(20, coll.find().count)
    verifyEq(10, coll.find().skip(10).count)
    cursor := coll.find([:], ["skip":10])
    verifyEq(10, cursor.count)
    10.times { cursor.next }
    verifyFalse(cursor.more)
    coll.drop
  }
  
  Void testToList()
  {
    coll := db["skiptest"]
    coll.drop
    verifyEq(0, coll.find().count)
    i := 0
    iter := 20
    iter.times { coll.insert(["v":i++]) }
    list := coll.find().toList
    verifyEq(iter, list.size)
    list.each |Str:Obj? val, j| {
      if(val["v"] != j)
        fail("cursor.toList() returned different values than expected")
    }
    coll.drop
  }
  
  Void testEach()
  {
    coll := db["eachtest"]
    coll.drop
    i := 0
    iter := 10
    iter.times { coll.insert(["count":i++]) }
    coll.find().each |v, c| {
      if(v["count"] != c)
        fail("cursor.each returned different values than expected")
    }
    
    coll.drop
  }
  
  Void testPartialObject()
  {
    coll := db["partialtest"]
    coll.drop
    o := ["field1":1, "field2":"two"]
    coll.insert(o)
    po := coll.findOne([:], ["fields":["field2"]])
    verify(po.keys.contains("field2"))
    verifyFalse(po.keys.contains("field1"))
    // for comparisons sake
    po = coll.findOne()
    verify(po.keys.containsAll(["field1", "field2"]))
    coll.drop
  }
  
  Void testExplain()
  {
    coll := db["explaintest"]
    coll.drop
    i := 0
    10.times { coll.insert(["something":i++]) }
    
    e := coll.find(["something":["\$gt":2]]).explain
    verify(e.keys.containsAll(["cursor", "allPlans", "nscannedObjects", "nscanned", "millis", "n"]))
    
    coll.drop
  }
  
  Void testSort()
  {
    coll := db["sorttest"]
    coll.drop
    20.times { coll.insert(["val":Int.random]) }
    
    verifyErr(ArgErr#) { coll.find().sort(["val":111]) }
    
    c := coll.find().sort(["val":1])
    previous := c.next["val"]
    verify(c.more)
    while(c.more) {
      current := c.next["val"]
      if(current < previous)
        fail()
      previous = current
    }
    
    // check descending
    c = coll.find().sort(["val":-1])
    previous = c.next["val"]
    verify(c.more)
    while(c.more) {
      current := c.next["val"]
      if(current > previous)
        fail()
      previous = current
    }
    
    coll.drop
  }

}