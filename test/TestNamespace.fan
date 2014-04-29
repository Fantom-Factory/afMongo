
internal class TestNamespace : MongoTest {
	Str longName := "AsheapproachedwiththosepastywhitearmshangingoutofhisGolavesthissmiletoldmeitwasbenefitdayndIknewmyvelourtracksuitwouldbehangingoffthelampshadetonight"

	Void testValidateDatabaseName() {
		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameCanNotBeEmpty("Database")) {
			Namespace.validateDatabaseName("")
		}

		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameTooLong("Database", longName, 64)) {
			Namespace.validateDatabaseName(longName)
		}

		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameHasInvalidChars("Database","Hey! > Yo!?", "/\\. \"*<>:|?")) {
			Namespace.validateDatabaseName("Hey! > Yo!?")
		}
	}

	Void testValidateCollectionName() {		
		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameCanNotBeEmpty("Collection")) {
			Namespace.validateCollectionName("")
		}

		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameHasInvalidChars("Collection", "\$tits", "\$")) {
			Namespace.validateCollectionName("\$tits")
		}
	}
	
	Void testValidateQname() {		
		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameCanNotBeEmpty("Namespace")) {
			Namespace.validateQname("")
		}

		verifyErrMsg(ArgErr#, ErrMsgs.namespace_nameTooLong("Namespace", longName,123)) {
			Namespace.validateQname(longName)
		}
	}
	
	Void testNameSplit() {
		ns := Namespace("hivezone.student")
		verifyEq(ns.databaseName, 	"hivezone")
		verifyEq(ns.collectionName, "student")
		verifyEq(ns.qname, "hivezone.student")
	}
}
