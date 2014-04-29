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
**  GridFS
**
@NoDoc
const class GridFS 
{
  static const Int DEFAULT_CHUNK_SIZE := 1024 * 256
  const DB db
  const Str root
  
  new make(DB db, Str root := "fs")
  {
    this.db = db
    this.root = root
    chunkColl.createIndex([["files_id": Mongo.ASCENDING], ["n": Mongo.ASCENDING]]);
  }
  
  CursorO fileList(Str:Obj? query := [:])
  {
    return filesColl.find(query).sort(["filename":1]);
  }
  
  internal CollectionO filesColl()
  {
    return db["${this.root}.files"]
  }
  
  internal CollectionO chunkColl()
  {
    return db["${this.root}.chunks"]
  }
  
  **
  ** Create a new file.
  ** You'll need to call 'GridFSFile.save' to actually store any data.
  **
  GridFSFile createFile(Str name, MimeType mt := MimeType.fromStr("text/plain"), Obj _id := ObjectId())
  {
    return GridFSFile(this, name, mt, _id)
  }
  
  GridFSFile? findOne(Str:Obj? query := [:])
  {
    o := filesColl.findOne(query)
    return (o == null) ? null : fix(o)
  }
  
  GridFSFile? findOneByName(Str filename)
  {
    return findOne(["filename":filename])
  }
  
  private GridFSFile fix(Str:Obj? o)
  {
    return GridFSFile(this, o["filename"], MimeType.fromStr(o["contentType"]), o["_id"]) {
      _size = o["length"]
      createdOn = o["uploadDate"]
      md5 = o["md5"]
      if(o.containsKey("metadata")) metadata = o["metadata"] 
      savedChunks = true // so additional data can't be uploaded
    }
  }
  
  Void remove(Str:Obj? query := [:])
  {
    filesColl.find(query).each |o| {
      removeID(o["_id"])
    }
  }
  
  Void removeID(ObjectId id)
  {
    filesColl.remove(["_id": id])
    chunkColl.remove(["files_id": id])
  }
  
  Void removeStr(Str filename)
  {
    remove(["filename": filename])
  }
  
  Void removeAll()
  {
    filesColl.remove()
    chunkColl.remove()
  }
}


