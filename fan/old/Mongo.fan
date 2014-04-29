using concurrent

@NoDoc
const class Mongo {
  const Int port
  const Str address
  internal const ConnectionO connection
  
  // op values
  static const Int OP_REPLY                    := 1
  static const Int OP_UPDATE                   := 2001
  static const Int OP_INSERT                   := 2002
  static const Int OP_QUERY                    := 2004
  static const Int OP_GET_MORE                 := 2005
  static const Int OP_DELETE                   := 2006
  static const Int OP_KILL_CURSORS             := 2007
  
  // for indexes and sorting
  static const Int ASCENDING := 1
  static const Int DESCENDING := -1

  new make(Str address := "127.0.0.1", Int port := 27017)
  { 
    this.address = address
    this.port    = port
    this.connection = ConnectionStoredInLocals(address, port)
  }

  DB db(Str name)
  {
    return DB(name, this)
  }
  
}
