using build

class Build : BuildPod {

	new make() {
		podName = "afMongo"
		summary = "A Fantom driver for MongoDB"
		version = Version("0.0.1")

		meta = [
			"proj.name"		: "Mongo",
			"repo.private"	: "true"
		]

		depends = [
			"sys 1.0", 
			"inet 1.0+",
			"concurrent 1.0+",

			"afConcurrent 1.0.2+",
			"afBson 0+"
		]
		
		srcDirs = [`test/`, `test/utils/`, `test/old/`, `fan/`, `fan/public/`, `fan/public/util/`, `fan/old/`, `fan/old/gridfs/`, `fan/internal/`]
		resDirs = [,]

		docApi = true
		docSrc = true
	}
}
