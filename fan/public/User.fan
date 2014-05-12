
** Represents a MongoDB user.
** 
** http://docs.mongodb.org/manual/reference/built-in-roles/
** Built in roles are:
**  - read
**  - readWrite
**  - dbAdmin
**  - dbOwner
**  - userAdmin
**  - clusterAdmin
**  - clusterManager
**  - clusterMonitor
**  - hostManager
**  - backup
**  - restore
**  - readAnyDatabase
**  - readWriteAnyDatabase
**  - userAdminAnyDatabase
**  - dbAdminAnyDatabase
**  - root
const class User {

	private const ConnectionManager conMgr
	private const Namespace	adminNs
	private const Namespace	userNs
	
	** The name of this user.
	const Str name

	internal new make(ConnectionManager conMgr, Str dbName, Str userName) {
		this.conMgr		= conMgr
		this.adminNs	= Namespace("admin", "system.users")
		this.userNs		= Namespace(dbName, "system.users")
		this.name		= userName
	}	
	
	** Returns user info.
	** FIXME: update to http://docs.mongodb.org/manual/reference/command/usersInfo/
	Str:Obj? info() {
		Collection(conMgr, adminNs).findOne(["db":userNs.databaseName, "user":name])
	}

	** Returns 'true' if this user exists.
	Bool exists() {
		Collection(conMgr, adminNs).findCount(["db":userNs.databaseName, "user":name]) > 0
	}

	** Creates the user with the given credentials. 
	**
	** @see `http://docs.mongodb.org/manual/reference/command/createUser/`
	[Str:Obj?] create(Str password, Str[] roles, [Str:Obj?]? customData := null) {
		cmd := cmd("insert")
			.add("createUser",	name)
			.add("pwd",			password)
		if (customData != null)	cmd["customData"] = customData
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
		Cmd(conMgr, userNs, action)
	}	
}
