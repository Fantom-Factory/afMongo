
@NoDoc
const mixin MongoConstants {
	
	// The default write concern all objects use if none supplied.
	static const Str:Obj? defaultWriteConcern := ["w": 1, "wtimeout": 0]

}
