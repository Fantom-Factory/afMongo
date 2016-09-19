using build

class Build : BuildPod {

	new make() {
		podName = "afMongo"
		summary = "A pure Fantom driver for MongoDB"
		version = Version("1.0.11")

		meta = [
			"pod.displayName"	: "Mongo",
			"repo.tags"			: "database",
			"repo.public"		: "true"
		]

		depends = [
			"sys        1.0.68 - 1.0", 
			"inet       1.0.68 - 1.0",
			"util       1.0.68 - 1.0",	// for Random()
			"concurrent 1.0.68 - 1.0",

			// ---- Core ------------------------
			"afConcurrent 1.0.12 - 1.0",
			"afBson       1.0.0  - 1.0"
		]

		srcDirs = [`fan/`, `fan/internal/`, `fan/public/`, `fan/public/util/`, `test/`, `test/db-tests/`, `test/unit-tests/`, `test/utils/`]
		resDirs = [`doc/`]
	}
}
