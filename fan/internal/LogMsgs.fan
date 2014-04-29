
internal const mixin LogMsgs {
	
	static Str cursor_indexOutOfSync(Int clientPos, Int serverPos) {
		"Client index '${clientPos}' and server index '${serverPos}' are out of sync!"
	}
	
}
