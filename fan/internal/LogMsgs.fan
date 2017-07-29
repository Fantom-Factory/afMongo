
internal const mixin LogMsgs {
	
	static Str cursor_indexOutOfSync(Int clientPos, Int serverPos) {
		"Client index '${clientPos}' and server index '${serverPos}' are out of sync!"
	}
	
	static Str connectionManager_unknownUrlOption(Str name, Str value, Uri mongoUrl) {
		"Unknown option in Mongo connection URL: ${name}=${value} - ${mongoUrl}"
	}
	
	static Str connectionManager_foundNewMaster(Uri mongoUrl) {
		"Found a new Master at ${mongoUrl}"
	}

	static Str connectionManager_waitingForConnectionsToClose(Int size, Uri mongoUrl) {
		"Waiting for ${size} connections to close on ${mongoUrl}..."
	}

	static Str connectionManager_waitingForConnectionsToFree(Int size, Uri mongoUrl) {
		"All ${size} are in use, waiting for one to become free on ${mongoUrl}..."
	}
}
