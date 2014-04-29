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
**  GridFSTest
**
class GridFSTest : OldMongoTest
{
  override Void setup()
  {
  
  }

  override Void teardown()
  {
  
  }
  
  Void NOtestBasic()
  {
    // todo - GridFSFile.isValid requires computing the md5,
    // which requires the non-existent crpyto pod
    gfs := GridFS(db)
    gfs.removeAll
    verifyEq(0, gfs.fileList.count, "gridfs isn't empty starting test")
    f := gfs.createFile("tester1")
    data := "allo there"
    f.save(data.toBuf.in)
    verifyEq(f.size, data.size)
    // verify(f.isValid) 
    
    
    bigfilepath := "/Users/liam/Documents/mtcode/fan/fantomongo/test/Ruckus.mp3"
    bigfile := File(Uri.fromStr(bigfilepath))
    if(!bigfile.exists)
      throw Err("please update fantomongo/test/GridFSTest.testBasic() with a large file that exists on your machine")
    verify(bigfile.size > GridFS.DEFAULT_CHUNK_SIZE, "must be large enough to test multiple chunks")
    verify(bigfile.mimeType != null, "mime type cannot be null")
    f2 := gfs.createFile("bigfiletest", bigfile.mimeType)
    f2.save(bigfile.in)
    verifyEq(bigfile.size, f2.size)
    // verify(f2.isValid)
    
    c := gfs.fileList
    verifyEq(2, c.count)
    c.each |o| {
      verify(o.keys.containsAll(["_id", "filename", "contentType", "length", "chunkSize", "uploadDate"]))
      // todo - also check for md5 when implemented
    }
    
    verifyNull(gfs.findOneByName("somegarbage"))
    
    f3 := gfs.findOneByName("tester1")
    verifyNotNull(f3)
    sb := Buf() // have to read to a Buf first since StrBuf won't allow writeBuf() on it
    f3.write(sb.out)
    sv := sb.readChars(sb.flip.size)
    
    path := bigfile.path.dup
    path[path.size - 1] = "ReadBack${bigfile.name}"
    leading := bigfile.uri.isPathAbs ? File.sep : "" // need to re-add the leading slash?
    readbackfile := File(Uri.fromStr("${leading}${path.join(File.sep)}"))
    if(readbackfile.exists)
      readbackfile.delete
    verify(readbackfile.parent == bigfile.parent)
    fmp3 := gfs.findOneByName("bigfiletest")
    verifyNotNull(fmp3)
    fmp3.write(readbackfile.out)
    verifyEq(readbackfile.mimeType, bigfile.mimeType)
    verifyEq(readbackfile.size, bigfile.size)
    
    gfs.removeAll
  }
  
}



