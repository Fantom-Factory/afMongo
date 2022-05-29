using build::BuildPod

class Build : BuildPod {

	new make() {
		podName = "afMongo"
		summary = "A pure Fantom driver for MongoDB"
		version = Version("2.1.1")

		meta = [
			"pod.dis"		: "Mongo",
			"repo.tags"		: "database",
			"repo.public"	: "true"
		]

		depends = [
			"sys          1.0.71 - 1.0", 
			"inet         1.0.71 - 1.0",
			"util         1.0.71 - 1.0",	// for Random() in MongoAuthScramSha1
			"concurrent   1.0.71 - 1.0",

			// ---- Core ------------------------
			"afConcurrent 1.0.26 - 1.0",
			"afBson       2.0.2  - 2.0",
		]

		srcDirs = [`fan/`, `fan/advanced/`, `fan/internal/`, `fan/public/`, `test/`, `test/db-tests/`, `test/unit-tests/`]
		resDirs = [`doc/`]
		
		meta["afBuild.uberPod"] = "afBson afConcurrent/Synchronized afConcurrent/SynchronizedState"
	}
}
