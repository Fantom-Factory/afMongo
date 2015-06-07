
** A Flag represents many states by setting and clearing bits on a Int. 
** 
** Using Ints as flags is still valid, but the Flags class gives superior debugging info. An 
** example Flags class:
** 
** pre>
** syntax: fantom
** 
** const class MyFlags : Flag {
**     static const MyFlags one     := MyFlags(1, "one")
**     static const MyFlags two     := MyFlags(2, "two")
**     static const MyFlags three   := MyFlags(3, "three")
**     static const MyFlags four    := MyFlags(4, "four")
** 
**     new make(|This|? f := null) : super(f) { }
**     new makeFromDefinition(Int flag, Str? name) : super(flag, name) { }
** }
** <pre
** 
** Set and clear bits by using '+' and '-' operators:
** 
** pre>
** syntax: fantom
** 
** (MyFlags.two + MyFlags.two) .toStr  --> two
** (MyFlags.two - MyFlags.four).toStr  --> two
** <pre
** 
** Multiple flags may be set:
** 
** pre>
** syntax: fantom
** 
** (MyFlags.one + MyFlags.four).toStr  --> one|four
** (MyFlags.two + MyFlags.four).toStr  --> two|four
** <pre
** 
** Flags are automatically coalesced:
** 
** pre>
** syntax: fantom
** 
** (MyFlags.one + MyFlags.three) .toStr  --> three 
** <pre
** 
** Unknown flags are presented as numbers:
** 
** pre>
** syntax: fantom
** 
** (MyFlags(16))               .toStr  --> (18)
** (MyFlags(10))               .toStr  --> two|(8)
** (MyFlags(27))               .toStr  --> three|(8)|(16)
** <pre
@NoDoc
abstract const class Flag {
	const Int value
	private const Str? pName
	
	Str name {
		get { pName == null ? computeName : pName }
		private set { }
	}
	
	protected new make(Int value, Str? name) {
		this.value = value
		this.pName = name
		if (name != null && name.isEmpty)
			throw ArgErr("Flag name can not be empty")
	}

	** Add Flag b.	Shortcut is a + b.
	@Operator 
	This plus(Flag b) {
		plusInt(b.value)
	}

	** Removes Flag b.	Shortcut is a - b.
	@Operator 
	This minus(Flag b) {
		minusInt(b.value)
	}

	** Add Flag b.	Shortcut is a + b.
	@Operator 
	This plusInt(Int b) {
		newValue	:= value.or(b)
		return (Flag) this.typeof.make([newValue, null])
	}

	** Removes Flag b.	Shortcut is a - b.
	@Operator 
	This minusInt(Int b) {
		newValue	:= value.and(b.not) 
		return (Flag) this.typeof.make([newValue, null])
	}
	
	** Returns 'true' if *any* of the given flag values are set on this object.
	Bool containsAny(Flag flag) {
		value.and(flag.value) > 0
	}

	** Returns 'true' if *all* the given flag values are set on this object.
	Bool containsAll(Flag flag) {
		value.and(flag.value) == flag.value
	}

	@NoDoc
	override Bool equals(Obj? obj) {
		(obj as Flag)?.value == value
	}
	
	@NoDoc
	override Int hash() {
		return value.hash
	}
	
	@NoDoc
	override Str toStr() {
		name
	}
	
	// ---- Private Methods ----------------------------------------------------------------------- 

	private Str computeName() {
		Flag[]
		match := [,]
		flags := this.findFlags
		value := this.value
		
		while (value > 0) {
			flag := flags.find |flag| {
				flag.value != 0 && flag.value.and(value) == flag.value
			}
			
			if (flag == null) {
				bit := findSetBits(value)[0]
				flag = ValueFlag(bit, "($bit)")
			}
			
			match.add(flag)
			value -= flag.value
		}

		if (match.isEmpty && !flags.isEmpty && flags[-1].value == 0)
			match.add(flags[-1])
		
		return match
			.sort |f1, f2| { ((Flag) f1).value <=> ((Flag) f2).value }
			.map |flag| { flag.pName }
			.join("|")
	}
	
	private Flag[] findFlags() {
		return typeof.fields
			.findAll |field| {
				field.isStatic && field.type == typeof
			}
			.map |field| {
				field.get
			}
			.sort |f1, f2| { 
				// inverse value order - required for finding composites
				((Flag) f2).value <=> ((Flag) f1).value
			}
	}
	
	private Int[] findSetBits(Int value) {
		// I'm gonna take a leap of faith that no-one uses the MSB of a 64 signed long!
		(63..0)
			.map |i| { 
				2.pow(i) 
			}
			.findAll |bit| {
				value.and(bit) == bit 
			} 
	}
}

internal const class ValueFlag : Flag {
	new make(Int flag, Str name) : super(flag, name) { }
}