
internal const mixin Utils {
	static const Str[]		ascSynonymns	:= "asc       ascending  up  north heaven wibble".lower.split
	static const Str[]		dscSynonymns	:= "dsc desc descending down south  hell  wobble".lower.split
	
	static const Str:Obj?	emptyDocument	:= [:] { ordered = true }
	
	** private static const Log log	:= Utils.getLog(Wotever#)
	static Log getLog(Type type) {
//		Log.get(type.pod.name + "." + type.name)
		type.pod.log
	}
	
	static [Str:Obj?] convertAscDesc(Str:Obj? doc) {
		doc.map |v| { 
			if (v isnot Str) return v
			if (ascSynonymns.contains((v as Str).lower)) return Cursor.ASC
			if (dscSynonymns.contains((v as Str).lower)) return Cursor.DESC
			return v
		}
	}
}
