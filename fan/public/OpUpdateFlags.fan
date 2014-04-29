
@NoDoc
const class OpUpdateFlags : Flag {
	static const OpUpdateFlags none			:= OpUpdateFlags(0, "None")
	
	static const OpUpdateFlags upsert		:= OpUpdateFlags(1.shiftl(0), "Upsert")
	static const OpUpdateFlags multiUpdate	:= OpUpdateFlags(1.shiftl(1), "MultiUpdate")
	
	@NoDoc
	new make(Int flag, Str? name) : super(flag, name) { }
}
