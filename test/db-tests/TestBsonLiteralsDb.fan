using afBson

internal class TestBsonLiteralsDb : MongoDbTest {
	private ObjectId objId 	:= ObjectId()
	private DateTime now	:= DateTime.now

	Void testBsonLiterals() {
		col := db["Literals"].drop
		col.insert(bsonValueMap)
		verifyBsonValueMap(col.findAll.first)
	}
	
	Map bsonValueMap() {
		[
			"double"		: 69f,
			"string"		: "string",
			"document"		: ["wot":"ever"],
			"array"			: ["wot","ever"],
			"binary-md5"	: Binary("dragon".toBuf, Binary.BIN_MD5),
			"binary-old"	: Binary("dragon".toBuf, Binary.BIN_BINARY_OLD),
			"binary-buf"	: "dragon".toBuf,
			"objectId"		: objId,
			"boolean"		: true,
			"date"			: now,
			"null"			: null,
			"regex"			: "wotever".toRegex,
			"code"			: Code("func() { ... }"),
			"code_w_scope"	: Code("func() { ... }", ["wot":"ever"]),
			"timestamp"		: Timestamp(500, 69),
			"int64"			: 666,
			"minKey"		: MinKey.val,
			"maxKey"		: MaxKey.val,
		]
	}

	Void verifyBsonValueMap(Map doc) {
		verifyEq(doc["double"], 	69f)
		verifyEq(doc["string"], 	"string")
		verifyEq(doc["document"]->get("wot"), "ever")
		verifyEq(doc["array"]->get(0), 	"wot")
		verifyEq(doc["array"]->get(1), 	"ever")
		verifyEq(doc["binary-md5"]->subtype,				Binary.BIN_MD5)
		verifyEq(doc["binary-md5"]->data->in->readAllStr,	"dragon")
		verifyEq(doc["binary-old"]->subtype,				Binary.BIN_BINARY_OLD)
		verifyEq(doc["binary-old"]->data->in->readAllStr,	"dragon")
		verifyEq(doc["binary-buf"]->readAllStr,				"dragon")
		verifyEq(doc["objectId"], 	objId)
		verifyEq(doc["boolean"], 	true)
		verifyEq(doc["date"], 		now)
		verifyEq(doc["null"], 		null)
		verifyEq(doc["regex"], 		"wotever".toRegex)
		verifyEq(doc["code"]->code,				"func() { ... }")
		verifyEq(doc["code"]->scope->isEmpty,	true)
		verifyEq(doc["code_w_scope"]->code,		"func() { ... }")
		verifyEq(doc["code_w_scope"]->scope,	Str:Obj?["wot":"ever"])
		verifyEq(doc["timestamp"],	Timestamp(500, 69))
		verifyEq(doc["int64"],		666)
		verifyEq(doc["minKey"],		MinKey.val)
		verifyEq(doc["maxKey"],		MaxKey.val)
	}
}
