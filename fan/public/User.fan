
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
** 
** Note in this API all user roles are bound to the containing database.
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
	
	** Returns info on the user.
	** 
	** @see `http://docs.mongodb.org/manual/reference/command/usersInfo/`
	[Str:Obj?] info() {
		cmd.add("usersInfo", ["user":name, "db":userNs.databaseName]).run["users"]->getSafe(0) ?: Str:Obj?[:]
	}

	** Returns 'true' if this user exists.
	Bool exists() {
		!info.isEmpty
	}

	** Creates the user with the given credentials. 
	**
	** @see `http://docs.mongodb.org/manual/reference/command/createUser/`
	This create(Str password, Str[] roles, [Str:Obj?]? customData := null) {
		cmd := cmd("insert")	// has writeConcern
			.add("createUser",	name)
			.add("pwd",			password)
		if (customData != null)	cmd["customData"] = customData
		cmd["roles"] = roles
		cmd.run
		// [ok:1.0]
		return this
	}

	** Drops this user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/dropUser/`
	This drop() {
		cmd.add("dropUser", name).run
		// [ok:1.0]
		return this
	}

	** Returns all roles held by this user.
	Str[] roles() {
		(([Str:Obj?][]) info["roles"]).findAll { it["db"] == userNs.databaseName }.map |role->Str| { role["role"] }.sort
	}
	
	** Grants roles to the user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/grantRolesToUser/`
	This grantRoles(Str[] roles) {
		cmd("update")	// has writeConcern
			.add("grantRolesToUser", name)
			.add("roles", roles)
			.run
		// [ok:1.0]
		return this
	}

	** Revokes roles from the user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/revokeRolesFromUser/`
	This revokeRoles(Str[] roles) {
		cmd("update")	// has writeConcern
			.add("revokeRolesFromUser", name)
			.add("roles", roles)
			.run
		// [ok:1.0]
		return this
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		Cmd(conMgr, userNs, action)
	}	
}
