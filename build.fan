using build

class Build : BuildPod {

	new make() {
		podName = "afMongo"
		summary = "A pure Fantom driver for MongoDB v2.6+"
		version = Version("0.0.5")

		meta = [
			"proj.name"		: "Mongo",
			"tags"			: "database",
			"repo.private"	: "true"
		]

		depends = [
			"sys 1.0", 
			"inet 1.0+",
			"concurrent 1.0+",

			"afConcurrent 1.0.4+",
			"afBson 1.0.0+"
		]

		srcDirs = [`test/`, `test/utils/`, `test/unit-tests/`, `test/db-tests/`, `fan/`, `fan/public/`, `fan/public/util/`, `fan/internal/`]
		resDirs = [,]
	}
}
