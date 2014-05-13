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
**  GridFSFile
**
@NoDoc
class GridFSFile
{
  const Obj _id
  Str name
  MimeType mimeType
  [Str:Obj?] metadata := [:]
  
  internal Int _size := 0
  internal GridFS gfs
  internal Str md5 := ""
  internal DateTime createdOn := DateTime.now
  internal Bool savedChunks := false
  
  new make(GridFS gfs, Str name, MimeType mt := MimeType.fromStr("text/plain"), Obj _id := ObjectId())
  {
    this._id = _id
    this.gfs = gfs
    this.name = name
    this.mimeType = mt
  }
  
  **
  ** Return the size in bytes of this file
  **
  Int size()
  {
    return _size
  }
  
  **
  ** Confirm, by checking with the DB, that this file is valid.
  ** note - this is not implemented, so will always return false for the moment, 
  ** even though the file is most likely valid (assuming no IOErrs, etc)
  **
  Bool isValid()
  {
    if (md5.isEmpty) return false
    res := gfs.db.command(["filemd5": _id])
    if(!DB.cmdOk(res))
      throw MongoOpErr("GridFSFile.isValid err - $res")
    return res["md5"] == this.md5
  }
  
  **
  ** Store data in this file.
  ** If this file has been saved before, only the metadata will
  ** be updated - returns false in this case, otherwise
  ** saves 'ins' to this file and returns true.
  **
  Bool save(InStream ins)
  {
    didSaveRaw := false
    if(!savedChunks){
      saveRawData(ins)
      didSaveRaw = true
    }
    gfs.filesColl.update(["_id":_id], this.toMap, true)
    return didSaveRaw
  }
  
  Int saveRawData(InStream ins)
  {
    b := Buf(GridFS.DEFAULT_CHUNK_SIZE)
    _size = 0
    chunkNum := 0
    
    // todo - calculate md5 as we go...something like the following in java
    // MessageDigest md = _md5Pool.get();
    // md.reset();
    // DigestInputStream in = new DigestInputStream( _in , md );
    
    more := true
    Int? v
    while (more) {
      while (b.size < b.capacity) {
        v = ins.readBuf(b, b.capacity - b.size)
        if(v == null || v == 0) { // should only have to check for null. fix after 1.0.49 is released...was a problem with Str.in
          more = false
          ins.close
          break
        }
        else
          _size += v
      }
      
      gfs.chunkColl.save(["files_id": _id, "n": chunkNum++, "data": b.flip])
      b.clear
    }
    savedChunks = true
    return chunkNum
  }
  
  Int numChunks()
  {
    f := _size.toFloat / GridFS.DEFAULT_CHUNK_SIZE.toFloat
    return f.ceil.toInt
  }
  
  Int write(OutStream out)
  {
    (0..<numChunks).each |i| { out.writeBuf(getChunk(i)) }
    return size
  }
  
  Void remove()
  {
    gfs.filesColl.remove(["_id": _id])
    gfs.chunkColl.remove(["files_id": _id])
  }
  
  internal Buf getChunk(Int i)
  {
    chunk := gfs.chunkColl.findOne(["files_id": _id, "n": i])
    if (chunk == null)
      throw MongoOpErr("can't find a chunk!  file id: $_id chunk: $i")
    return chunk["data"]
  }
  
  Str:Obj toMap()
  {
    Str:Obj m := [:] { ordered = true }
    m["_id"] = _id
    m["filename"] = name
    m["contentType"] = mimeType.toStr
    m["length"] = _size
    m["chunkSize"] = GridFS.DEFAULT_CHUNK_SIZE
    m["uploadDate"] = createdOn
    // if(false) m["aliases"] = ["test", "test2"] // this isn't support in other drivers yet...
    if(metadata.size > 0) m["metadata"] = metadata
    m["md5"] = md5
    return m
  }
  
  override Str toStr()
  {
    return this.toMap.toStr
  }
}

// todo - add ChunkInStream and ChunkOutStream to read/write directly into GridFS

