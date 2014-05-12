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
**  DBTest
**
class DBTest : OldMongoTest 
{
  const Str testuser := "fantester"
  const Str testpass := "fanpass"
  
  override Void setup()
  {
    if(!db.users().contains(testuser))
      db.addUser(testuser, testpass)
  }
  
  override Void teardown()
  {
    db.removeUser(testuser)
    // db.drop
  }
  
  Void testProfiling()
  {
    proflevel := db.profilingLevel
    verify((0..2).contains(proflevel))
    newlevel := proflevel + 1
    if (newlevel > 2) newlevel = 0
    db.setProfilingLevel(newlevel)
    verify(newlevel == db.profilingLevel)
    db.setProfilingLevel(proflevel) // reset it
    
    s := db.profilingInfo
    echo("profilingInfo - ${s}")
  }
  
  Void testAuthentication()
  {
    db.removeUser(testuser)
    verifyFalse(db.users().contains(testuser))
    db.addUser(testuser, testpass)
    verify(db.users().contains(testuser))
    verify(db.authenticate(testuser, testpass))
  }
  
  Void testBuildInfo()
  {
    verify(db.buildInfo().keys().containsAll(["version", "gitVersion", "sysInfo", "bits"]))
  }
  
  Void testErrs()
  {
    db.resetErrorHistory
    verifyNull(db.lastErr)
    verifyNull(db.previousErr)

    db.command(["forceerror": 1])
    verifyNotNull(db.lastErr)
    verifyNotNull(db.previousErr)

    db.command(["forceerror": 1])
    verifyNotNull(db.lastErr)
    verifyNotNull(db.previousErr)
    prevErr := db.previousErr
    verifyEq(1, prevErr["nPrev"])
    verifyEq(prevErr["err"], db.lastErr)
    
    db["test"].findOne
    verifyNull(db.lastErr)
    verifyNotNull(db.previousErr)
    verifyEq(2, db.previousErr["nPrev"])
    
    db.resetErrorHistory
    verifyNull(db.lastErr)
    verifyNull(db.previousErr)
  }
  
  Void testCollNames()
  {
    newcoll := "newcoll"
    fullname := "${db.name}.${newcoll}"
    verifyFalse(db.collectionNames().contains(fullname))
    db[newcoll].insert(["rando":"tester"])
    verify(db.collectionNames().contains(fullname))
    db.dropCollection(newcoll)
    verifyFalse(db.collectionNames().contains(fullname))
  }
  
  Void testServerStatus()
  {
    ss := db.serverStatus()
    verify(ss.keys.containsAll(["uptime", "globalLock", "mem"]))
  }
  
  Void testListDatabases()
  {
    db.listDatabases.each |Map v, i| {
      verify(v.keys.containsAll(["name", "sizeOnDisk", "empty"]))
    }
  }
  
//  Void testRepair()
//  {
//    db.repair(true, true) // just run it...make sure no Errs are thrown
//  }
  
  Void testEval()
  {
    verifyEq(3f, db.eval("function (x) {return x;}", [3f]))
    
    // make sure the test DB is dropped
    verifyNull(db.eval("function () {db.test_eval.drop();}"))
    verifyNull(db.eval("function (x) {db.test_eval.save({y:x});}", [5f]))
    verifyEq(5f, db["test_eval"].findOne()["y"])
    
    verifyEq(5f, db.eval("function (x, y) {return x + y;}", [2, 3]))
    verifyEq(5f, db.eval("function () {return 5;}"))
    verifyEq(5f, db.eval("2 + 3;"))
    // 
    // verifyEq(5f, db.eval(Code.new("2 + 3;"))
    // verifyEq(2f, db.eval(Code.new("return i;", {"i" => 2}))
    // verifyEq(5f, db.eval(Code.new("i + 3;", {"i" => 2}))
    
    verifyErr(MongoOpErr#) { db.eval("5 ++ 5;") }
  }
  
}


