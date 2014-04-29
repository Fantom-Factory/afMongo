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
**  DB
**
@NoDoc
const class DB 
{
  const Str name
  internal const ConnectionO connection
  
  new make(Str name, Mongo mongo)
  {
    this.name = name
    this.connection = mongo.connection
  }

  new makeFromConnection(Str name, ConnectionO connection)
  {
    this.name = name
    this.connection = connection
  }
  
  **
  ** Add a new user to the list of authenticable users
  **
  Void addUser(Str username, Str password)
  {
    users := CollectionO(this, "system.users")
    Str:Obj? u := users.findOne(["user": username]) ?: ["user": username]
    u["pwd"] = pwdHash(username, password)
    users.save(u)
  }
  
  **
  ** Remove a user from the list of authenticable users
  **
  Void removeUser(Str username)
  {
    CollectionO(this, "system.users").remove(["user": username])
  }
  
  **
  ** Return a list of all authenticable users
  **
  Str[] users()
  {
    return CollectionO(this, "system.users").find.toList.map |Str:Obj? o->Str|{
      return o["user"]
    }
  }
  
  **
  ** Log into the DB - required for certain operations.
  ** The user must already exist, either via `DB.addUser` or some other mechanism.
  **
  Bool authenticate(Str username, Str password)
  {
    res := command(["getnonce": 1])
    if (!cmdOk(res))
      throw Err("authenticate - error retrieving nonce: ${res}")

    nonce := res["nonce"]
    auth := [:] { ordered = true }
    auth.set("authenticate", 1).set("user", username).set("nonce", nonce) 
    s := "${nonce}${username}${pwdHash(username, password)}"
    auth["key"] = Buf().print(s).toDigest("MD5").toHex
    
    return cmdOk(command(auth))
  }
  
  private Str pwdHash(Str username, Str password)
  {
    return Buf().print("${username}:mongo:${password}").toDigest("MD5").toHex
  }
  
  **
  ** Logout from a DB session after having logged in via authenticate()
  **
  Bool logout()
  {
    res := command(["logout": 1])
    return cmdOk(res)
  }
  
  CollectionO collection(Str name)
  {
    return CollectionO(this, name)
  }
  
  // so we can say db["collname"]
  @Operator
  CollectionO get(Str name)
  {
    return CollectionO(this, name)
  }
  
  **
  ** Return a list of all the collections in this DB.
  **
  Str[] collectionNames()
  {
    names := Str[,]
    CollectionO(this, "system.namespaces").find.toList.each |v, i| {
      Str s := (v as Map)["name"]
      if (!s.contains("system.") && !s.contains("\$"))
        names.add(s)
    }
    return names
  }
  
  Bool renameCollection(Str from, Str to)
  {
    return true
  }
  
  **
  ** Return the current profiling level.
  ** Can be between 0-2.  See `DB.setProfilingLevel` to set it.
  **
  Int profilingLevel()
  {
    res := command(["profile":-1])
    if (!cmdOk(res))
      throw MongoOpErr("""error while retrieving profiling level - """ + res["err"])
    return res["was"]
  }
  
  **
  ** Set the profiling level.
  ** 'level' must be one of the following options
  **  - 0 - off
  **  - 1 - only slow
  **  - 2 - all
  **
  Void setProfilingLevel(Int level)
  {
    if (!(0..2).contains(level))
      throw ArgErr("invalid profiling level ${level} - must be between 0 - 2.")
    command(["profile": level])
  }
  
  List profilingInfo()
  {
    return CollectionO(this, "system.profile").find.toList
  }
  
  Str? lastErr()
  {
    res := command(["getlasterror": 1])
    if (!cmdOk(res))
      throw MongoOpErr("lastErr() failure - ${res}")
    return res["err"]
  }
  
  **
  ** 
  **
  Map? previousErr()
  {
    Str:Obj? res := command(["getpreverror": 1])
    return (res["err"] != null) ? res : null
  }
  
  **
  ** Reset the DB error history as far as previousErr() and 
  ** lastStatus() are concerned
  **
  Void resetErrorHistory()
  {
    command(["reseterror": 1])
  }
  
  **
  ** Retrieve build info about the MongoDB instance being interacted with.
  ** Keys in the Map returned include "version", "gitVersion", "sysInfo", and "bits"
  **
  Str:Obj buildInfo()
  {
    res := command(["buildinfo": 1], true)
    if (!cmdOk(res))
      throw MongoOpErr("""invalid buildInfo request - """ + res["err"])
    return res
  }
  
  Str:Obj validateCollection(Str coll)
  {
    res := command(["validate": "${name}.${coll}"])
    if (!cmdOk(res))
//      throw MongoOpErr("""Error with validate command: """ + res["err"])
      throw MongoOpErr("""Error with validate command: """ + res)
    
    result := res["result"]
    if (result isnot Str)
      throw MongoOpErr("Error with validation data: ${res}")
    // raise "Error: invalid collection #{name}: #{doc.inspect}" if result =~ /\b(exception|corrupt)\b/i
    return res
  }
  
  **
  ** Evaluate a JavaScript snippet server-side.
  ** This can be helpful when you want to reduce network bandwidth for low-touch operations.
  ** If your 'javascript' Str is a function that accepts arguments, they can be passed in
  ** as the 'args' parameter.
  **
  ** Mongo provides a server side 
  ** [JS api]`http://mongodb.onconfluence.com/display/DOCS/mongo+-+The+Interactive+Shell`
  ** for finding/updating/deleting objects, which is available to you in an eval script.
  **
  Obj? eval(Str javascript, List args := [,])
  {
    cmd := [:] { ordered = true }
    cmd["\$eval"] = javascript
    if (!args.isEmpty) cmd["args"] = args
    doc := command(cmd)
    if (cmdOk(doc))
      return doc["retval"]
    else
      throw MongoOpErr("eval failed: ${doc}")
  }
  
  Map? command(Map cmd, Bool admin := false)
  {
    if (cmd.keys.size > 1 && !cmd.ordered)
      throw ArgErr("commands with more than one key must be ordered")
    // negative batchsize means "return the abs value specified and close the cursor" 
    Str:Obj opts := ["batchsize": -1]
    if (admin == true) opts["admin"] = true
    return collection("\$cmd").findOne(cmd, opts)
  }
  
  internal static Bool cmdOk(Str:Obj? cmd)
  {
    ok := cmd["ok"]
    return (ok == 1f) || (ok == true)
  }
  
  ** 
  ** Get information on the indexes for the given collection.
  ** Normally called by Collection.indexInfo. Returns a hash where
  ** the keys are index names (as returned by Collection.createIndex) and
  ** the values are lists of 'fieldname, direction' pairs specifying the index
  ** (as passed to Collection.createIndex).
  **
  Str:Obj indexInfo(Str coll)
  {
    info := [:]
    idxs := collection("system.indexes").find(["ns": "${this.name}.${coll}"])
    idxs.each |v, i| {
      info[v["name"]] = v["key"]
    }
    return info
  }

  **
  ** Returns true if index exists in collection
  **
  Bool indexExists(Str coll, Str idxName)
  {
     idx := collection("system.indexes").findOne(["ns": "${this.name}.${coll}", "name": "$idxName"])
     return idx != null
  }
  
  Void dropIndex(Str coll, Str idx)
  {
    cmd := Str:Obj?[:] { ordered = true }
    cmd.set("deleteIndexes", coll).set("index", idx) 
    res := command(cmd)
    if (!cmdOk(res))
      throw MongoOpErr("dropIndex failed: ${res}")
  }
  
  Void dropCollection(Str coll)
  {
    if (collectionNames.contains("${name}.${coll}")) {
      res := command(["drop":coll])
      if (!cmdOk(res))
        throw MongoOpErr("drop collection failed: ${res}")
    }
  }
  
  **
  ** Drop this database - careful!
  **
  Void drop()
  {
    res := command(["dropDatabase": 1])
    if (!cmdOk(res))
      throw MongoOpErr("dropDatabase failed: ${res}")
  }
  
  **
  ** List all databases known on the DB server
  ** Each item returned is a Map with the Str keys: "name", "sizeOnDisk" and "empty"
  ** Note - you must be authenticated for this to succeed
  **
  List listDatabases()
  {
    res := command(["listDatabases": 1], true)
    if (!cmdOk(res))
      throw MongoOpErr("listDatabases failed: ${res}")
    return res["databases"]
  }
  
  **
  ** Returns status info about the DB server - uptime, lock info, memory info
  **
  Map serverStatus()
  {
    // weird...doesn't return an "ok" in the response...just return it directly
    return command(["serverStatus": 1])
  }
  
  **
  ** Repair/compact this DB.
  **
  Void repair(Bool preserveClonedFilesOnFailure, Bool backupOriginalFiles)
  {
    c := Str:Obj [:] { ordered = true }
    c["repairDatabase"] = 1
    c["preserveClonedFilesOnFailure"] = preserveClonedFilesOnFailure
    c["backupOriginalFiles"] = backupOriginalFiles
    res := command(c)
    if (!cmdOk(res))
      throw MongoOpErr("repairDatabase error - $res")
  }
  
}
