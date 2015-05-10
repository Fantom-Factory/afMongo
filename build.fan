using build

class Build : BuildPod {

	new make() {
		podName = "afMongo"
		summary = "A pure Fantom driver for MongoDB"
		version = Version("1.0.4")

		meta = [
			"proj.name"		: "Mongo",
			"tags"			: "database",
			"repo.private"	: "false"
		]

		depends = [
			"sys 1.0", 
			"inet 1.0",
			"concurrent 1.0",

			// ---- Core ------------------------
			"afConcurrent 1.0.6 - 1.0",
			"afBson       1.0.0 - 1.0"
		]

		srcDirs = [`test/`, `test/utils/`, `test/unit-tests/`, `test/db-tests/`, `fan/`, `fan/public/`, `fan/public/util/`, `fan/internal/`]
		resDirs = [,]
	}
}
