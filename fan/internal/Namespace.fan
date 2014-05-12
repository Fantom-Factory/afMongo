
** Represents a MongoDB namespace.
** 
** @see `http://docs.mongodb.org/manual/reference/limits/#naming-restrictions`
internal const class Namespace {
	
	** These are the invalid chars for Windows, *nix has fewer chars. But we don't know which 
	** system MongoDB is running on without connecting and making a 'buildinfo' cmd call. So 
	** better we just assume the lowest common denominator -> Windows.
	**  
	** Who would put dodgy chars in a DB name anyway!?
	private static const Int[] invalidNameChars	:= "/\\. \"*<>:|?".chars

	const Str databaseName
	const Str collectionName
	const Str qname
	
	new make(Str qname) {
		dot := qname.index(".")
		this.databaseName	= validateDatabaseName(qname[0..<dot])
		this.collectionName = validateCollectionName(qname[dot+1..-1])
		this.qname			= validateQname("${databaseName}.${collectionName}")
	}

	new makeFromSplit(Str databaseName, Str collectionName) {
		this.databaseName 	= validateDatabaseName(databaseName)
		this.collectionName = validateCollectionName(collectionName)
		this.qname			= validateQname("${databaseName}.${collectionName}")
	}

	Bool isSystem() {
		collectionName.startsWith("system.")
	}
	
	Namespace withCollection(Str collectionName) {
		Namespace(databaseName, collectionName)
	}
	
	static Str validateDatabaseName(Str name) {
		if (name.isEmpty)
			throw ArgErr(ErrMsgs.namespace_nameCanNotBeEmpty("Database"))
		
		if (name.toBuf.size >= 64)
			throw ArgErr(ErrMsgs.namespace_nameTooLong("Database", name, 64))
		
		if (name.any { invalidNameChars.contains(it) })
			throw ArgErr(ErrMsgs.namespace_nameHasInvalidChars("Database", name, Str.fromChars(invalidNameChars)))

		return name
	}
	
	static Str validateCollectionName(Str name) {
		if (name.isEmpty)
			throw ArgErr(ErrMsgs.namespace_nameCanNotBeEmpty("Collection"))
		
		if (name.any { it == '$' })
			throw ArgErr(ErrMsgs.namespace_nameHasInvalidChars("Collection", name, "\$"))

		return name
	}

	static Str validateQname(Str name) {
		if (name.isEmpty)
			throw ArgErr(ErrMsgs.namespace_nameCanNotBeEmpty("Namespace"))

		if (name.toBuf.size >= 123)
			throw ArgErr(ErrMsgs.namespace_nameTooLong("Namespace", name, 123))

		return name
	}
}
