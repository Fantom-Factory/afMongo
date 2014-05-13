
internal class TestMongoClientDb : MongoDbTest {
	
	Void testTestDiagnostics() {
		verify(mc.listDatabases.size > 1)
		
		verify(mc.databaseNames.contains("admin"))

		verify(mc.listCommands.keys.contains("listCommands"))
		
		verify(Version(mc.buildInfo["version"].toStr) >= Version("2.6"))

		verify(mc.hostInfo.containsKey("system"))
		
		verify(mc.serverStatus.containsKey("host"))
	}
}
