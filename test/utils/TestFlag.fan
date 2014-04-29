
class TestFlags : Test {
	
	Void testSingle() {
		verifyEq(MyFlags(1, null).name, "one")
		verifyEq(MyFlags(2, null).name, "two")
		verifyEq(MyFlags(4, null).name, "four")
	}

	Void testDouble() {
		verifyEq(MyFlags(5, null).name, "one|four")
		verifyEq(MyFlags(6, null).name, "two|four")
	}

	Void testTriple() {
		verifyEq(MyFlags(13, null).name, "one|four|eight")
		verifyEq(MyFlags(14, null).name, "two|four|eight")
	}

	Void testComposite() {
		verifyEq(MyFlags( 3, null).name, "three")
		verifyEq(MyFlags( 7, null).name, "three|four")
		verifyEq(MyFlags(11, null).name, "three|eight")
		verifyEq(MyFlags(15, null).name, "three|four|eight")
	}
	
	Void testUnknown() {
		verifyEq(MyFlags( 16, null).name, "(16)")
		verifyEq(MyFlags( 17, null).name, "one|(16)")
		verifyEq(MyFlags( 18, null).name, "two|(16)")
		verifyEq(MyFlags( 19, null).name, "three|(16)")
		verifyEq(MyFlags( 31, null).name, "three|four|eight|(16)")
		verifyEq(MyFlags( 48, null).name, "(16)|(32)")
	}
	
	Void testNaught() {
		verifyEq(MyFlags( 0, null).name, "naught")		
	}
	
	Void testAdd() {
		verifyEq(MyFlags.one + MyFlags.two,  MyFlags.three)		

		// 2 + 2 = 2!!! I always knew it!
		verifyEq(MyFlags.two + MyFlags.two,  MyFlags.two)		
	}

	Void testSub() {
		verifyEq(MyFlags.three - MyFlags.two,  MyFlags.one)

		// 4 - 2 = 4!!! I always knew it!
		verifyEq(MyFlags.four - MyFlags.two,  MyFlags.four)		
	}

	Void testSubMultiple() {
		verifyEq(MyFlags.three - MyFlags.three,  MyFlags.naught)
	}

	Void testContainsAny() {
		verify(MyFlags.one.containsAny(MyFlags.three))
		verify(MyFlags.two.containsAny(MyFlags.three))
		
		verify(MyFlags.three.containsAny(MyFlags.one))
		verify(MyFlags.three.containsAny(MyFlags.two))

		verify(!MyFlags.one.containsAny(MyFlags.two))
		verify(!MyFlags.two.containsAny(MyFlags.one))
	}

	Void testContainsAll() {
		verify(!MyFlags.one.containsAll(MyFlags.three))
		verify(!MyFlags.two.containsAll(MyFlags.three))
		
		verify(MyFlags.three.containsAll(MyFlags.one))
		verify(MyFlags.three.containsAll(MyFlags.two))
		verify(MyFlags.three.containsAll(MyFlags.three))

		verify(!MyFlags.one.containsAny(MyFlags.two))
		verify(!MyFlags.two.containsAny(MyFlags.one))
	}
}

internal const class MyFlags : Flag {
	static const MyFlags eight		:= MyFlags(8, "eight")
	static const MyFlags one		:= MyFlags(1, "one")
	static const MyFlags two		:= MyFlags(2, "two")
	static const MyFlags three		:= MyFlags(3, "three")
	static const MyFlags four		:= MyFlags(4, "four")
	static const MyFlags naught		:= MyFlags(0, "naught")

	new make(Int flag, Str? name) : super(flag, name) { }
}

