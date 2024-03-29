using afBson::BsonIO

internal class TestScram : Test {
	
	Void testDecodeConversation() {
		data := Int[	0x00, 0x00, 0x00, 0x00, 0x74, 0x65,		0x73, 0x74, 0x2e, 0x24, 0x63, 0x6d, 0x64, 0x00,
			0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,		0x61, 0x00, 0x00, 0x00, 0x10, 0x73, 0x61, 0x73,
			0x6c, 0x53, 0x74, 0x61, 0x72, 0x74, 0x00, 0x01,		0x00, 0x00, 0x00, 0x02, 0x6d, 0x65, 0x63, 0x68,
			0x61, 0x6e, 0x69, 0x73, 0x6d, 0x00, 0x0c, 0x00,		0x00, 0x00, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d,
			0x53, 0x48, 0x41, 0x2d, 0x31, 0x00, 0x05, 0x70,		0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x00, 0x24,
			0x00, 0x00, 0x00, 0x00, 0x6e, 0x2c, 0x2c, 0x6e,		0x3d, 0x74, 0x65, 0x73, 0x74, 0x2c, 0x72, 0x3d,
			0x40, 0x41, 0x74, 0x2e, 0x56, 0x7d, 0x45, 0x3d,		0x3c, 0x34, 0x66, 0x29, 0x46, 0x42, 0x2f, 0x50,
			0x52, 0x37, 0x41, 0x57, 0x72, 0x3c, 0x26, 0x77,		0x00]
		
		secret := Buf().with |buf| { data.each { buf.write(it) } }.flip

		flags	:= secret.in.readS4
		qname	:= secret.in.readNullTerminatedStr
		skip	:= secret.in.readS4
		limit	:= secret.in.readS4
		query	:= BsonIO().readDoc(secret.in)
		
		verifyEq(query["saslStart"], 1)
		verifyEq(query["mechanism"], "SCRAM-SHA-1")
		verifyEq(query.containsKey("payload"), true)
	}
	
	Void testScramSha1() {
		val := Buf.fromBase64("dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTA9").readAllStr
		verifyEq(val, "v=UMWeI25JD1yNYZRMpZ4VHvhZ9e0=")
	}
}
