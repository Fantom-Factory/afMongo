
** Represents a MongoDB user.
** 
** To enable authentication, ensure MongoDB is started in *secure* mode:
** 
**   C:\> mongod --auth
** 
** If you don't then all users have access to all databases.
** 
** Users are assigned *roles*. [Built-in roles]`http://docs.mongodb.org/manual/reference/built-in-roles/` 
** for all databases are:
** 
**  - read
**  - readWrite
**  - dbAdmin
**  - dbOwner
**  - userAdmin
**
** Built in roles for the admin database are:
** 
**  - readWriteAnyDatabase
**  - userAdminAnyDatabase
**  - dbAdminAnyDatabase
** 
** To create a [superuser]`http://stackoverflow.com/questions/20117104/mongodb-root-user`, or root 
** user, grant her ALL of the above admin database roles. (It's always handy to have one!)
** 
** Note in this API all user roles are bound to the containing database.
const class User {

	private const ConnectionManager conMgr
	private const Namespace	userNs
	
	** The name of this user.
	const Str name
	
	** Creates a 'User' with the given details.
	new make(ConnectionManager conMgr, Str dbName, Str userName, |This|? f := null) {
		f?.call(this)
		this.conMgr		= conMgr
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
	This create(Str password, Str[] roles, [Str:Obj?]? customData := null, [Str:Obj?]? writeConcern := null) {
		cmd("insert")	// has writeConcern
			.add("createUser",		name)
			.add("pwd",				password)
			.add("customData",		customData)
			.add("roles",			roles)
			.add("writeConcern",	writeConcern ?: conMgr.writeConcern)
			.run
		// [ok:1.0]
		return this
	}

	** Drops this user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/dropUser/`
	This drop(Bool checked := true) {
		if (checked || exists) cmd.add("dropUser", name).run
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
	This grantRoles(Str[] roles, [Str:Obj?]? writeConcern := null) {
		cmd("update")	// has writeConcern
			.add("grantRolesToUser", 	name)
			.add("roles", 				roles)
			.add("writeConcern",		writeConcern ?: conMgr.writeConcern)
			.run
		// [ok:1.0]
		return this
	}

	** Revokes roles from the user.
	**
	** @see `http://docs.mongodb.org/manual/reference/command/revokeRolesFromUser/`
	This revokeRoles(Str[] roles, [Str:Obj?]? writeConcern := null) {
		cmd("update")	// has writeConcern
			.add("revokeRolesFromUser", name)
			.add("roles", 				roles)
			.add("writeConcern",		writeConcern ?: conMgr.writeConcern)
			.run
		// [ok:1.0]
		return this
	}
	
	// ---- Private Methods -----------------------------------------------------------------------
	
	private Cmd cmd(Str? action := null) {
		return Cmd(conMgr, userNs, action)
	}

	// ---- Obj Overrides -------------------------------------------------------------------------
	
	@NoDoc
	override Str toStr() {
		"${userNs.databaseName}::${name}"
	}

}
