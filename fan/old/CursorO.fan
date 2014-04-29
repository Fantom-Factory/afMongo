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
**  Cursor
**
@NoDoc
class CursorO 
{
  private static const Int CURSOR_NOT_FOUND :=    1.shiftl(0)
  private static const Int QUERY_FAILURE :=       1.shiftl(1)
  private static const Int SHARD_CONFIG_STALE :=  1.shiftl(2)
  private static const Int AWAIT_CAPABLE :=       1.shiftl(3)
  
  private static const Log log := Log.get("mongo")
  internal const CollectionO coll
  // cannot be const, as Code is not const
  Map selector            // document selector
  Map opts                      // query options
  private Int itemsSeen := -1   // how many items have we seen? -1 indicates we haven't even queried
  private Int cursorID := 0     // DB assigned cursor ID
  private List cache := [,]     // cache of returned objects
  private Bool closed := false  // set to true when the DB tells us there's nothing left
  
  new make(CollectionO c, Map selector, Map opts)
  {
    this.coll = c
    this.selector = selector
    this.opts = opts
  }
  
  **
  ** Return true if this cursor has remaining data, false if not.
  **
  Bool more()
  {
    fillErUp
    return cache.size > 0
  }
  
  **
  ** Get the next element from this cursor.
  ** Returns null if no more elements are available.
  **
  Map? next()
  {
    fillErUp
    return (cache.size > 0) ? cache.removeAt(0) : null
  }
  
  // get more data if needed
  private Void fillErUp()
  {
    if (!closed && cache.size == 0) {
      if (itemsSeen < 0) { // we haven't sent our initial query yet
        itemsSeen = 0
        doQuery(numToReturn)
      }
      else {
        num := numToReturn
        if (num <= opts.get("limit", num) && num >= 0)
          getMore(num)
      }
    }
  }
  
  // a return value less than zero indicates that we shouldn't even try to fetch anything
  private Int numToReturn()
  {
    num := opts.get("batchsize", 0) // default to 0 which lets the DB decide how much to send back
    if (opts.containsKey("limit")) {
      num = ((Int)opts["limit"] - itemsSeen)
      if (num == 0)
        num = -1
    }
    return num
  }
  
  **
  ** Set the limit for the number of results returned by this cursor.
  ** If it has already returned more than the limit passed in,
  ** this has no effect.
  **
  This limit(Int lim)
  {
    if (lim > itemsSeen)
      opts["limit"] = lim
    return this
  }
  
  **
  ** Skip to an offset into the cursor.
  ** Note that this is only meaningful before the cursor has retrieved
  ** any documents (once more() or next() have been called).  
  **
  This skip(Int offset)
  {
    opts["skip"] = offset
    return this
  }
  
  **
  ** The count of items available from this cursor.
  ** Note that this does not take into account any items that
  ** have already been retrieved by this cursor - ie, count will not
  ** decrease as you call next().
  ** It does, however, take into account limit() and skip()
  **
  Int count()
  {
    cmd := Str:Obj[:] { ordered = true }
    cmd.set("count", coll.name).set("query", selector).set("fields", opts["fields"])
    res := coll.db.command(cmd)
    if (DB.cmdOk(res)) {
      c := (res["n"] as Float)?.toInt
      if (c == null)
        throw MongoOpErr("cannot find \"n\" in response")
      return c.min(opts.get("limit", c)).minus(opts.get("skip", 0))
    } 
    else if (res["errmsg"] == "ns missing")
      return 0 
    else
      throw MongoOpErr("count() failed - $res")
  }
  
  **
  ** Sort the results of this query on the given fields.
  ** The Int value for each field specifies the sort direction, 
  ** either Mongo.ASCENDING (1) or Mongo.DESCENDING (-1).  Other values are invalid.
  **
  This sort(Str:Int fields)
  {
    if (![1, -1].containsAll(fields.vals))
      throw ArgErr("sort direction can only be ascending (1) or descending (-1)")
    opts["order"] = fields
    return this
  }
  
  **
  ** Return an explanation of how this Cursor's query will be executed,
  ** with regard to index usage, etc.
  **
  Str:Obj explain()
  {
    eopts := this.opts.dup
    eopts.set("explain", true).set("batchsize", -1)
    c := CursorO(coll, this.selector, eopts)
    explanation := c.next
    c.close
    return explanation
  }
  
  **
  ** Returns a List with the fully retrieved contents of this Cursor
  ** Note - if your query matches a lot of results, this could consume
  ** quite a lot of memory.
  **
  List toList()
  {
    objects := Map[,]
    while (this.more)
      objects.add(next)
    return objects
  }
  
  Void each(|Str:Obj? item, Int index| c)
  {
    i := 0
    while (this.more) { c.callList([this.next, i++]) }
  }
  
  **
  ** Which query options have been selected.
  ** See http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
  ** 
  private Int queryOpts()
  {
    timeout := 0 // opts["timeout"] ? 0 : MongoOp.QUERY_NO_CURSOR_TIMEOUT
    slaveOk := 0 // @connection.slave_ok? ? MongoOp.QUERY_SLAVE_OK : 0 
    return slaveOk + timeout
  }
  
  private Str fullName()
  {
    return (opts["admin"] == true) ? "admin.${coll.name}" : coll.fullName
  }
  
  private [Str:Int]? fieldsForQuery(Str[] fields)
  {
    if (fields.isEmpty) return null
    m := Str:Int[:]
    fields.each |val| { m[val] = 1 }
    return m
  }
  
  private Str:Obj specialFields()
  {
    Str:Obj special := [:] { ordered = true }
    if (opts.containsKey("explain"))
      special["\$explain"] = true
    if (opts.containsKey("order"))
      special["orderby"] = opts["order"]
    // todo snapshot, hint and order
    return special
  }
  
  private Void doQuery(Int numToRetrieve)
  {
    b := Buf() { endian = Endian.little }
    b.writeI4(queryOpts)                        // query opts
	fn := fullName
    BsonWriter(b.out).writeCString(fn)       // full name
    b.writeI4(opts.get("skip", 0))              // skip
    b.writeI4(numToRetrieve)                    // num to return
    
    sf := specialFields()
    if (sf.size > 0) {
      sf["query"] = selector
      BsonWriter(b.out).writeDocument(sf)		// query object
      // no need to check for fields in this case
    }
    else {
      BsonWriter(b.out).writeDocument(selector)	// query object
      if (opts.containsKey("fields"))              // optional fieldReturnSelector
        BsonWriter(b.out).writeDocument(fieldsForQuery(opts["fields"]))
    }
    
    s := coll.db.connection.getSocket
    reqID := coll.db.connection.sendMsg(s.out, b.flip, Mongo.OP_QUERY)
    readResponse(s.in, reqID)
  }
  
  private Void getMore(Int numToRetrieve)
  {
    b := Buf() { endian = Endian.little }
    b.writeI4(0)                          // reserved
    BsonWriter(b.out).writeCString(fullName) // full name
    b.writeI4(numToRetrieve)              // num to return
    b.writeI8(this.cursorID)              // cursor ID
    
    s := coll.db.connection.getSocket
    reqID := coll.db.connection.sendMsg(s.out, b.flip, Mongo.OP_GET_MORE)
    readResponse(s.in, reqID)
  }
  
  Void close()
  {
    // if we don't have a good ID, or we've read through to completion, and received
    // a cursorID of 0 as a result, no need to talk to the DB - just mark our state
    if (this.cursorID != 0) {
      b := Buf() { endian = Endian.little }
      b.writeI4(0)                          // reserved
      b.writeI4(1)                          // number of cursors
      b.writeI8(this.cursorID)              // cursor ID
      
      s := coll.db.connection.getSocket
      coll.db.connection.sendMsg(s.out, b.flip, Mongo.OP_KILL_CURSORS)
    }
    cursorID = 0
    closed = true
  }
  
  **
  ** Read a response back after having sent a message.
  ** Update our cursorID, and add any return objects
  ** to our cache.
  **
  private Void readResponse(InStream ins, Int requestID)
  {
    // standard header
    ins.skip(8) // eat the length and the request ID
    if (requestID != ins.readS4)
      log.warn("Connection - mismatching request/response IDs")
    if (Mongo.OP_REPLY != ins.readS4)
      log.warn("Connection - unexpected opcode from DB")
    // end standard header
    
    // handle response flags
    responseFlags := ins.readS4 // bit mask of status
    // todo - handle any errors here better
    if (responseFlags.and(CURSOR_NOT_FOUND) != 0)
      log.warn("Connection - cursor not found: $responseFlags")
    if (responseFlags.and(QUERY_FAILURE) != 0)
      log.warn("Connection - query failure: $responseFlags")
    if (responseFlags.and(SHARD_CONFIG_STALE) != 0)
      log.warn("Connection - shard config stale: $responseFlags")
    // todo - manage AWAIT_CAPABLE properly
    // if (!responseFlags.and(AWAIT_CAPABLE))
      // log.warn("Connection - not await capable: $responseFlags")
    // end of response flags handling
    
    if ((cursorID = ins.readS8) == 0) // this reply had everything in it...nothing more to get
      close
    startingFrom := ins.readS4
    numberReturned := ins.readS4
    itemsSeen += numberReturned
    // Sys.out.printLine("cursorID - ${cursorID}, startingFrom - ${startingFrom}, numberReturned - ${numberReturned}")
    
    while (numberReturned-- > 0)
      cache.add(BsonReader(ins).readDocument)
  }
  
}
