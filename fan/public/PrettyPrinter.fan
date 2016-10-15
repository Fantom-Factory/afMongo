using afBson

** Pretty prints MongoDB documents to a JSON-esque string.
** Useful for debugging.
** 
** Note PrettyPrinter only pretty prints if the resulting text string if greater than 'maxWidth'.
** So if 'PrettyPrinter' appears not to be working, then try setting a smaller 'maxWidth'.
** 
**   syntax: fantom
**   str := PrettyPrinter { maxWidth=20 }.print(mongoDoc)
** 
const class PrettyPrinter {

	** Set to 'false' to disable pretty printing and have documents printed on a single line.
	const Bool	prettyPrint	:= true
	
	** The indent string used to indent the document.
	const Str 	indent		:= "  "

	** The maximum width of a list or map before it is broken up into separate lines.
	const Int 	maxWidth	:= 80
	
	private const PrettyPrintOptions?	_ppOpts
	
	** Creates a 'PrettyPrinter' with the default pretty printing options. 
	** May be either a 'PrettyPrintOptions' instance, or just 'false' to disable pretty printing. 
	** 
	**   syntax: fantom
	**   printer := PrettyPrinter()
	**   printer := PrettyPrinter { it.indent="\t"; it.maxWidth=40; }
	new make(|This|? f := null) {
		f?.call(this)
		
		if (prettyPrint) {
			_ppOpts = PrettyPrintOptions {
				it.maxWidth	= this.maxWidth
				it.indent	= this.indent
			}
		}
	}
	
	** Pretty prints the given MongoDB object / document to a 'Str'.
	** 
	**   syntax: fantom
	**   str := prettyPrinter.print(mongoDoc)
	** 
	Str print(Obj? obj) {
		buf := StrBuf()
		printToStream(obj, buf.out)
		return buf.toStr
	}

	** Pretty prints the given MongoDB object / document to a stream.
	** 
	**   syntax: fantom
	**   prettyPrinter.print(mongoDoc, out)
	** 
	This printToStream(Obj? obj, OutStream out) {
		ctx := JsonWriteCtx(out, _ppOpts)
		_writeJsonToStream(ctx, obj)
		ctx.finalise
		return this
	}

	** A simple override hook to alter values *before* they are printed.
	** 
	** By default this just returns the given value.  
	virtual Obj? convertHook(Obj? val) { val }
	
	@NoDoc
	override Str toStr() {
		"PrettyPrinter { indent=${indent.toCode}, maxWidth=${maxWidth.toCode} }"
	}
	
	// ---- private methods -----------------------------------------------------------------------

	private This _writeJsonToStream(JsonWriteCtx ctx, Obj? obj) {
		obj = convertHook(obj)
			 if (obj is Str)		_writeJsonStr		(ctx, obj)
		else if (obj is Map)		_writeJsonMap		(ctx, obj)
		else if (obj is List)		_writeJsonList		(ctx, obj)
		else if (obj is Binary)		_writeBsonBinary	(ctx, obj)
		else if (obj is MinKey)		_writeBsonMinKey	(ctx, obj)
		else if (obj is MaxKey)		_writeBsonMaxKey	(ctx, obj)
		else if (obj is ObjectId)	_writeBsonObjId		(ctx, obj)
		else if (obj is Timestamp)	_writeBsonTimestamp	(ctx, obj)
		else if (obj == null)		_writeJsonNull		(ctx)
		else 						_writeObj			(ctx, obj)
		return this
	}
	
	private Void _writeJsonMap(JsonWriteCtx ctx, Map map) {
		ctx.objectStart
		notFirst := false
		map.each |val, key| {
			if (key isnot Str) throw Err("MongoDB map key is not Str type: $key [$key.typeof]")
			if (notFirst) ctx.objectVal
			_writeJsonStr(ctx, key)
			ctx.objectKey
			_writeJsonToStream(ctx, val)
			notFirst = true
		}
		ctx.objectEnd
	}

	private Void _writeJsonList(JsonWriteCtx ctx, Obj?[] array) {
		ctx.arrayStart
		notFirst := false
		array.each |item| {
			if (notFirst) ctx.arrayItem
			_writeJsonToStream(ctx, item)
			notFirst = true
		}
		ctx.arrayEnd
	}

	private Void _writeJsonStr(JsonWriteCtx ctx, Str str) {
		ctx.valueStart
		ctx.writeChar(JsonToken.quote)
		str.each |char| {
			if (char <= 0x7f) {
				switch (char) {
					case '\b': ctx.writeChar('\\').writeChar('b')
					case '\f': ctx.writeChar('\\').writeChar('f')
					case '\n': ctx.writeChar('\\').writeChar('n')
					case '\r': ctx.writeChar('\\').writeChar('r')
					case '\t': ctx.writeChar('\\').writeChar('t')
					case '\\': ctx.writeChar('\\').writeChar('\\')
					case '"' : ctx.writeChar('\\').writeChar('"')
					
					// note '/' may be escaped but doesn't have to be
					// see http://stackoverflow.com/questions/1580647/json-why-are-forward-slashes-escaped
					//case '/' : ctx.writeChar('\\').writeChar('/')
					default	 : ctx.writeChar(char)
				}
			}
			else {
				ctx.writeChar('\\').writeChar('u').print(char.toHex(4))
			}
		}
		ctx.writeChar(JsonToken.quote)
		ctx.valueEnd
	}

	private Void _writeJsonNull(JsonWriteCtx ctx) {
		ctx.valueStart.print("null").valueEnd
	}

	private Void _writeBsonBinary(JsonWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((Binary) obj).toJs ).valueEnd
	}

	private Void _writeBsonMinKey(JsonWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((MinKey) obj).toJs ).valueEnd
	}

	private Void _writeBsonMaxKey(JsonWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((MaxKey) obj).toJs ).valueEnd
	}

	private Void _writeBsonObjId(JsonWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((ObjectId) obj).toJs ).valueEnd
	}

	private Void _writeBsonTimestamp(JsonWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((Timestamp) obj).toJs ).valueEnd
	}

	private Void _writeObj(JsonWriteCtx ctx, Obj obj) {
		ctx.valueStart.print(obj).valueEnd
	}
}

internal mixin JsonWriteCtx {
	static new make(OutStream out, Obj? prettyPrintOptions) {
		if (prettyPrintOptions != null) {
			if (prettyPrintOptions == false)
				return JsonWriteCtxUgly(out)

			ppOpts := prettyPrintOptions == true ? PrettyPrintOptions() : prettyPrintOptions 
			return JsonWriteCtxPretty(out, ppOpts)
		}
		return JsonWriteCtxUgly(out)
	}
	
	abstract This valueStart()
	abstract This print(Obj s)
	abstract This writeChar(Int char)
	abstract This valueEnd()
	
	abstract Void arrayStart()
	abstract Void arrayItem()
	abstract Void arrayEnd()

	abstract Void objectStart()
	abstract Void objectKey()
	abstract Void objectVal()
	abstract Void objectEnd()

	abstract Void finalise()
}

internal class JsonWriteCtxPretty : JsonWriteCtx {
	private OutStream 			out
	private PrettyPrintOptions	ppOpts
	private Int 				indent		:= 0
	
	private JsonValWriter?		last
	private JsonValWriter[]		valWriters	:= JsonValWriter[,]

	new make(OutStream out, PrettyPrintOptions ppOpts) {
		this.out	= out
		this.ppOpts	= ppOpts
	}
	
	override This print(Obj s) {
		valWriters.peek.writeJson(s)
		return this
	}
	
	override This writeChar(Int ch) {
		valWriters.peek.writeChar(ch)
		return this
	}

	override This valueStart()	{ valWriters.push(JsonValWriterLit(ppOpts)); return this }
	override This valueEnd()	{ writerEnd	}
	
	override Void arrayStart()	{ valWriters.push(JsonValWriterList(ppOpts)) }
	override Void arrayItem()	{ }
	override Void arrayEnd()	{ writerEnd	}
	
	override Void objectStart()	{ valWriters.push(JsonValWriterMap(ppOpts)) }
	override Void objectKey()	{ }
	override Void objectVal()	{ }
	override Void objectEnd()	{ writerEnd	}
	
	override Void finalise()	{ out.writeChars(last.str) }
	
	private This writerEnd() {
		last = valWriters.pop
		peek := valWriters.peek
		peek?.add(last.str)
		return this
	}
}

internal abstract class JsonValWriter {
	PrettyPrintOptions	ppOpts

	new make(PrettyPrintOptions ppOpts) {
		this.ppOpts	= ppOpts
	}
	
	virtual  Void writeJson(Obj ob) { throw Err("WTF?") }
	virtual  Void writeChar(Int ch)	{ throw Err("WTF?") }
	virtual  Void add(Str item)		{ throw Err("WTF?")	}
	abstract Str  str()
}

internal class JsonValWriterLit : JsonValWriter {
	private StrBuf	value	:= StrBuf(32)
	
	new make(PrettyPrintOptions ppOpts) : super(ppOpts) { }

	override Void writeJson(Obj ob)	{ value.add(ob)	}
	override Void writeChar(Int ch)	{ value.addChar(ch)	}
	override Str str() 				{ value.toStr		}
}

internal class JsonValWriterList : JsonValWriter {
	private Int		size	:= 1
	private Str[]	list	:= Str[,]

	new make(PrettyPrintOptions ppOpts) : super(ppOpts) { }

	override Void add(Str item)	{
		list.add(item)
		size += item.size + 2
	}

	override Str str() {
		size -= 2
		size += 1
		if (size > ppOpts.maxWidth) {
			// bufSize is only approx unless we start counting the lines in items
			bufSize := size + (list.size * ppOpts.indent.size * 2)
			json := StrBuf(bufSize)
			json.addChar(JsonToken.arrayStart).addChar('\n')
			list.each |item, i| {
				lines := item.splitLines
				lines.each |line, j| {
					json.add(ppOpts.indent).add(line)
					if (j < lines.size-1)
						json.addChar('\n')
				}
				if (i < list.size - 1)
					json.addChar(JsonToken.comma)
				json.addChar('\n')
			}
			json.addChar(JsonToken.arrayEnd)
			return json.toStr
		} else
			return "[" + list.join(", ") + "]"
	}
}

internal class JsonValWriterMap : JsonValWriter {	
	private Str[]	keys		:= Str[,]
	private Str[]	vals		:= Str[,]
	private Int		size		:= 1
	private Int		maxKeySize	:= 0
	private Int		maxValSize	:= 0
	
	new make(PrettyPrintOptions ppOpts) : super(ppOpts) { }

	override Void add(Str item) {
		(keys.size > vals.size ? vals : keys).add(item)
		size += item.size + 2
		if (keys.size > vals.size)
			maxKeySize = maxKeySize.max(item.size)
		else
			maxValSize = maxValSize.max(item.size)
	}

	override Str str() {
		size -= 2
		size += 1
		maxKeySize := maxKeySize + 1
		if (size > ppOpts.maxWidth) {
			// bufSize is only approx unless we start counting the lines in vals
			bufSize := (keys.size * maxKeySize) + (vals.size * maxValSize) + (keys.size * ppOpts.indent.size * 2)
			json := StrBuf(bufSize)
			json.addChar(JsonToken.objectStart).addChar('\n')
			
			keys.each |key, i| {
				val := vals[i]
				
				json.add(ppOpts.indent)
				json.add(key.justl(maxKeySize))
				json.addChar(JsonToken.colon)
				json.addChar(' ')
				
				lines := val.splitLines
				json.add(lines.first)
				if (lines.size > 1)
					lines.eachRange(1..-1) |line, j| {
						json.addChar('\n')
						json.add(ppOpts.indent).add(line)
					}
				if (i < keys.size - 1)
					json.addChar(JsonToken.comma)
				json.addChar('\n')
			}
			
			json.addChar(JsonToken.objectEnd)
			return json.toStr

		} else {
			json := StrBuf(size)
			json.addChar(JsonToken.objectStart)
			keys.each |key, i| {
				val := vals[i]
				json.add(key).addChar(JsonToken.colon).addChar(' ').add(val)
				if (i < keys.size - 1)
					json.addChar(JsonToken.comma).addChar(' ')
			}
			json.addChar(JsonToken.objectEnd)
			return json.toStr
		}
	}
}

internal class JsonWriteCtxUgly : JsonWriteCtx {
	private OutStream	out

	new make(OutStream out) {
		this.out = out
	}
	
	override This print(Obj s) {
		out.print(s)
		return this
	}
	
	override This writeChar(Int char) {
		out.writeChar(char)
		return this
	}
	
	override This valueStart()		{ this									}
	override This valueEnd()		{ this									}
	
	override Void arrayStart()		{ out.writeChar(JsonToken.arrayStart)	}
	override Void arrayItem()		{ out.writeChar(JsonToken.comma)		}
	override Void arrayEnd()		{ out.writeChar(JsonToken.arrayEnd)		}

	override Void objectStart()		{ out.writeChar(JsonToken.objectStart)	}
	override Void objectKey()		{ out.writeChar(JsonToken.colon)		}
	override Void objectVal()		{ out.writeChar(JsonToken.comma)		}
	override Void objectEnd()		{ out.writeChar(JsonToken.objectEnd)	}

	override Void finalise()		{ 										}
}

internal mixin JsonToken {
	static const Int objectStart	:= '{'
	static const Int objectEnd		:= '}'
	static const Int colon			:= ':'
	static const Int arrayStart		:= '['
	static const Int arrayEnd		:= ']'
	static const Int comma			:= ','
	static const Int quote			:= '"'
}

internal const class PrettyPrintOptions {

	** The indent string used to indent the document.
	const Str 	indent		:= "  "

	** The maximum width of a list or map before it is broken up into separate lines.
	const Int 	maxWidth	:= 80	

	** Default 'it-block' ctor.
	new make(|This|? in := null) { in?.call(this) }
	
	override Str toStr() {
		"indent=${indent.toCode}, maxWidth = ${maxWidth}"
	}
}
