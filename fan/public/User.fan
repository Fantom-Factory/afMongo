
** Represents a MongoDB user.
const class User {

	private const ConnectionManager conMgr
	private const Namespace	namespace
	
	** The name of this user.
	const Str name

	internal new make(ConnectionManager conMgr, Str dbName, Str userName) {
		this.conMgr		= conMgr
		this.namespace	= Namespace(dbName, "system.users")
		this.name		= userName
	}	
	
	** Creates the user with the given credentials. 
	**
	** @see `http://docs.mongodb.org/manual/reference/command/createUser/`
	[Str:Obj?] create(Str password, Str[] roles, [Str:Obj?]? customData := null) {
		cmd := cmd("insert")
			.add("createUser",	name)
			.add("pwd",			password)
		if (customData != null)	cmd["customData"] 	= customData
		cmd["roles"] = roles
		return cmd.run
	}

	** Drops this user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/dropUser/`
	[Str:Obj?] drop() {
		cmd("drop").add("dropUser", name).run
	}

	** Grants roles to the user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/grantRolesToUser/`
	[Str:Obj?] grantRoles(Str userName, Str[] roles) {
		cmd("update")
			.add("grantRolesToUser", userName)
			.add("roles", roles)
			.run
	}

	** Revokes roles from the user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/revokeRolesFromUser/`
	[Str:Obj?] revokeRoles(Str userName, Str[] roles) {
		cmd("update")
			.add("grantRolesToUser", userName)
			.add("roles", roles)
			.run
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, namespace, action)
	}	
}
