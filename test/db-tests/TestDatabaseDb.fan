using afBson

internal class TestDatabaseDb : MongoDbTest {
	
	Void testEval() {
		 verifyEq(db.eval(Code("function (x) { return x + y; }", ["y":2]), [3f]), 5f)
		 verifyEq(db.eval(Code("function ( ) { return null;  }"), [ ,]), null)
		 verifyEq(db.eval(Code("function (x) { return {'x':x+1 };  }"), [6f]), Str:Obj?["x":7f])
	}
	
	Void testDiagnostics() {
		verifyEq(db.stats(3)["db"], db.name)
	}
}
