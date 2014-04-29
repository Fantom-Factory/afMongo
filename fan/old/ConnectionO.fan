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


using inet
using concurrent

**
** Connection
**
@NoDoc
abstract const class ConnectionO
{
  private const Int port
  private const Str address

  private static const AtomicInt idCounter := AtomicInt(0)
  
  new make(Str address, Int port)
  {
    this.address = address
    this.port    = port
  }
  
  TcpSocket getSocket()
  {
    s := socket
    if (!s.isConnected) {
      s.connect(IpAddr(address), port)
      s.in.endian = s.out.endian = Endian.little
    }
    return s
  }
  
  Int sendMsg(OutStream os, Buf b, Int op)
  {
    reqId := idCounter.incrementAndGet
    // header - len, reqID, reserved, opcode
    os.writeI4(b.size + 16).writeI4(reqId).writeI4(0).writeI4(op)
    os.writeBuf(b).flush
    return reqId
  }
  
  Void close()
  {
    socket(false)?.close
  }
	
  abstract protected TcpSocket? socket(Bool forceCreate := true)
}

internal const class ConnectionStoredInLocals : ConnectionO {
  private const Str id

  new make(Str address, Int port) : super(address, port) {
    this.id = "MongoConn-" + Int.random.toHex
  }

  override TcpSocket? socket(Bool forceCreate := true) {
    TcpSocket? s := Actor.locals[id]
    if (s == null && forceCreate) {
      s = TcpSocket()
      Actor.locals[id] = s
    }
    return s
  }
}
