using afBson::ObjectId

internal class TestQuery : MongoDbTest {
	
	MongoColl?	col
	
	override Void setup() {
		super.setup
		col = db["testQuery"]
		col.drop
		col.insert([
			"_id"	: ObjectId(),
			"name"	: "Judge",
			"value"	: 16,
		])
		col.insert([
			"_id"	: ObjectId(),
			"name"	: "Dredd",
			"value"	: 19,
		])
		col.insert([
			"_id"	: ObjectId(),
			"name"	: "Wotever",
			"value"	: 20,
		])
		col.index("_text_").ensure(["name":MongoIdx.TEXT])
	}
	
	Void testExample() {
		q := MongoQ {
			and(
				or(eq("price", 0.99f), eq("price", 1.99f)),
				or(eq("price", 0.99f), eq("price", 1.99f))
			)
		}.dump
		p :=
		"{
		   \$and : [
		     {\$or: [{price: 0.99}, {price: 1.99}]},
		     {\$or: [{price: 0.99}, {price: 1.99}]}
		   ]
		 }"	
		verifyEq(p, q.print)
	}

	// ---- Comparison Query Operators ------------------------------------------------------------

	
	private [Str:Obj?][] query(|MongoQ| qfn) {
		q := MongoQ()
		qfn(q)
		return col.find(q.query).toList
	}

	Void testEq() {
		res := query {
			eq("name", "Judge")
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Judge")
	}

	Void testNotEq() {
		res := query {
			notEq("name", "Judge")
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Dredd")
		verifyEq(res[1]["name"], "Wotever")
	}

	Void testIn() {
		res := query {
			in("name", "Judge Judy".split)
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Judge")
	}
	
	Void testNotIn() {
		res := query {
			notIn("name", "Judge Wotever".split)
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Dredd")
	}
	
	Void testGreaterThan() {		
		res := query {
			greaterThan("value", 19)
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Wotever")
		verifyEq(res.first["value"], 20)
	}
	
	Void testGreaterThanOrEqualTo() {		
		res := query { 
			greaterThanOrEqTo("value", 19)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["value"], 19)
		verifyEq(res[1]["value"], 20)
	}
	
	Void testLessThan() {
		res := query {
			lessThan("value", 19)
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["value"], 16)
	}
	
	Void testLessThanOrEqualTo() {		
		res := query {
			lessThanOrEqTo("value", 19)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["value"], 16)
		verifyEq(res[1]["value"], 19)
	}
	
	// ---- Element Query Operators ---------------------------------------------------------------

	Void testExists() {		
		res := query { exists("value") }
		verifyEq(res.size, 3)

		res = query { exists("value", false) }
		verifyEq(res.size, 0)

		res = query { exists("foobar") }
		verifyEq(res.size, 0)

		res = query { exists("foobar", false) }
		verifyEq(res.size, 3)
	}

	// ---- String Query Operators ----------------------------------------------------------------

	Void testEqIgnoreCase() {
		res := query {
			eqIgnoreCase("name", "judge")
		}
		verifyEq(res.size, 1)
		verifyEq(res[0]["name"], "Judge")
	}
	
	Void testContains() {
		res := query {
			contains("name", "ud", false)
		}
		verifyEq(res.size, 1)
		verifyEq(res[0]["name"], "Judge")

		res = query {
			contains("name", "RE", true)
		}
		verifyEq(res.size, 1)
		verifyEq(res[0]["name"], "Dredd")
	}
	
	Void testStartsWith() {
		col.insert([
			"_id"	: ObjectId(),
			"name"	: "Dreddnought",
			"value"	: -1,
		])
		res := query {
			startsWith("name", "Dredd", false)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Dredd")
		verifyEq(res[1]["name"], "Dreddnought")

		res = query {
			startsWith("name", "DREDD", true)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Dredd")
		verifyEq(res[1]["name"], "Dreddnought")
	}

	Void testEndsWith() {
		col.insert([
			"_id"	: ObjectId(),
			"name"	: "Neverever",
			"value"	: -1,
		])
		res := query {
			endsWith("name", "ever", false)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Wotever")
		verifyEq(res[1]["name"], "Neverever")

		res = query {
			endsWith("name", "EVER", true)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Wotever")
		verifyEq(res[1]["name"], "Neverever")
	}
	
	// ---- Logical Query Operators --------------------------------------------------------------
	
	Void testAnd() {
		res := query {
			and(
				eq("name", "Dredd"),
				eq("value", 19)
			)
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Dredd")
	}

	Void testOr() {
		res := query {
			or(
				eq("name", "Judge"),
				eq("name", "Dredd")
			)
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Judge")
		verifyEq(res[1]["name"], "Dredd")
	}

	Void testNot() {
		res := query {
			in("name", ["Dredd"]).not
		}
		verifyEq(res.size, 2)
		verifyEq(res[0]["name"], "Judge")
		verifyEq(res[1]["name"], "Wotever")
	}

	Void testNor() {
		res := query {
			nor(
				eq("name", "Judge"),
				eq("name", "Dredd")
			)
		}
		verifyEq(res.size, 1)
		verifyEq(res[0]["name"], "Wotever")
	}

	// ---- Evaluation Query Operators ------------------------------------------------------------

	Void testMod() {
		res := query {
			mod("value", 8, 0)
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Judge")
		verifyEq(res.first["value"], 16)
	}

	Void testWhere() {
		res := query {
			where("this.name == 'Dredd'")
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Dredd")
	}

	Void testTextSearch() {
		res := query {
			textSearch("Dredd")
		}
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Dredd")
		
		res = col.textSearch("Dredd").toList
		verifyEq(res.size, 1)
		verifyEq(res.first["name"], "Dredd")
	}
	
	// ---- Sort Tests ----------------------------------------------------------------------------
	
	Void testSort() {
		col.insert([
			"_id"	: ObjectId(),
			"name"	: "Dredd",
			"value"	: 22,
		])
		res := col.find(null) {
			it->sort = [:] {ordered=true}.add("name", 1).add("value", -1)
		}.toList
		verifyEq(res.size, 4)
		verifyEq(res[0]["name"],  "Dredd")
		verifyEq(res[0]["value"], 22)
		verifyEq(res[1]["name"],  "Dredd")
		verifyEq(res[1]["value"], 19)
		verifyEq(res[2]["name"],  "Judge")
		verifyEq(res[3]["name"],  "Wotever")
	}
}
