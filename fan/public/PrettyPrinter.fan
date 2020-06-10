using afBson::Binary
using afBson::MinKey
using afBson::MaxKey
using afBson::ObjectId
using afBson::Timestamp

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
	
	** Creates a 'PrettyPrinter'. Use an it-block to set pretty printing options. 
	** 
	**   syntax: fantom
	**   printer := PrettyPrinter { it.indent="\t"; it.maxWidth=40; }
	new make(|This|? f := null) {
		f?.call(this)
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
		ctx := (MongoWriteCtx) (prettyPrint ? MongoWriteCtxPretty(out, indent, maxWidth) : MongoWriteCtxUgly(out))
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

	private This _writeJsonToStream(MongoWriteCtx ctx, Obj? obj) {
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
	
	private Void _writeJsonMap(MongoWriteCtx ctx, Map map) {
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

	private Void _writeJsonList(MongoWriteCtx ctx, Obj?[] array) {
		ctx.arrayStart
		notFirst := false
		array.each |item| {
			if (notFirst) ctx.arrayItem
			_writeJsonToStream(ctx, item)
			notFirst = true
		}
		ctx.arrayEnd
	}

	private Void _writeJsonStr(MongoWriteCtx ctx, Str str) {
		ctx.valueStart
		ctx.writeChar(MongoValWriter.quote)
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
		ctx.writeChar(MongoValWriter.quote)
		ctx.valueEnd
	}

	private Void _writeJsonNull(MongoWriteCtx ctx) {
		ctx.valueStart.print("null").valueEnd
	}

	private Void _writeBsonBinary(MongoWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((Binary) obj).toJs ).valueEnd
	}

	private Void _writeBsonMinKey(MongoWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((MinKey) obj).toJs ).valueEnd
	}

	private Void _writeBsonMaxKey(MongoWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((MaxKey) obj).toJs ).valueEnd
	}

	private Void _writeBsonObjId(MongoWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((ObjectId) obj).toJs ).valueEnd
	}

	private Void _writeBsonTimestamp(MongoWriteCtx ctx, Obj obj) {
		ctx.valueStart.print( ((Timestamp) obj).toJs ).valueEnd
	}

	private Void _writeObj(MongoWriteCtx ctx, Obj obj) {
		ctx.valueStart.print(obj).valueEnd
	}
}

internal mixin MongoWriteCtx {
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

	virtual Str ppIndent()   { "" }
	virtual Int ppMaxWidth() { -1 }
}

internal class MongoWriteCtxPretty : MongoWriteCtx {
	private OutStream 			out
	private Int 				indent		:= 0
	
	private MongoValWriter?		last
	private MongoValWriter[]	valWriters	:= MongoValWriter[,]

	override Str				ppIndent
	override Int				ppMaxWidth

	new make(OutStream out, Str ppIndent, Int ppMaxWidth) {
		this.out		= out
		this.ppIndent	= ppIndent
		this.ppMaxWidth	= ppMaxWidth
	}
	
	override This print(Obj s) {
		valWriters.peek.writeJson(s)
		return this
	}
	
	override This writeChar(Int ch) {
		valWriters.peek.writeChar(ch)
		return this
	}

	override This valueStart()	{ valWriters.push(MongoValWriterLit(this)); return this }
	override This valueEnd()	{ writerEnd	}
	
	override Void arrayStart()	{ valWriters.push(MongoValWriterList(this)) }
	override Void arrayItem()	{ }
	override Void arrayEnd()	{ writerEnd	}
	
	override Void objectStart()	{ valWriters.push(MongoValWriterMap(this)) }
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

internal abstract class MongoValWriter {
	static const Int objectStart	:= '{'
	static const Int objectEnd		:= '}'
	static const Int colon			:= ':'
	static const Int arrayStart		:= '['
	static const Int arrayEnd		:= ']'
	static const Int comma			:= ','
	static const Int quote			:= '"'

	MongoWriteCtx	ppOpts

	new make(MongoWriteCtx ppOpts) {
		this.ppOpts	= ppOpts
	}
	
	virtual  Void writeJson(Obj ob) { throw Err("WTF?") }
	virtual  Void writeChar(Int ch)	{ throw Err("WTF?") }
	virtual  Void add(Str item)		{ throw Err("WTF?")	}
	abstract Str  str()
}

internal class MongoValWriterLit : MongoValWriter {
	private StrBuf	value	:= StrBuf(32)
	
	new make(MongoWriteCtx ppOpts) : super(ppOpts) { }

	override Void writeJson(Obj ob)	{ value.add(ob)	}
	override Void writeChar(Int ch)	{ value.addChar(ch)	}
	override Str str() 				{ value.toStr		}
}

internal class MongoValWriterList : MongoValWriter {
	private Int		size	:= 1
	private Str[]	list	:= Str[,]

	new make(MongoWriteCtx ppOpts) : super(ppOpts) { }

	override Void add(Str item)	{
		list.add(item)
		size += item.size + 2
	}

	override Str str() {
		size -= 2
		size += 1
		if (size > ppOpts.ppMaxWidth) {
			// bufSize is only approx unless we start counting the lines in items
			bufSize := size + (list.size * ppOpts.ppIndent.size * 2)
			json := StrBuf(bufSize)
			json.addChar(MongoValWriter.arrayStart).addChar('\n')
			list.each |item, i| {
				lines := item.splitLines
				lines.each |line, j| {
					json.add(ppOpts.ppIndent).add(line)
					if (j < lines.size-1)
						json.addChar('\n')
				}
				if (i < list.size - 1)
					json.addChar(MongoValWriter.comma)
				json.addChar('\n')
			}
			json.addChar(MongoValWriter.arrayEnd)
			return json.toStr
		} else
			return "[" + list.join(", ") + "]"
	}
}

internal class MongoValWriterMap : MongoValWriter {	
	private Str[]	keys		:= Str[,]
	private Str[]	vals		:= Str[,]
	private Int		size		:= 1
	private Int		maxKeySize	:= 0
	private Int		maxValSize	:= 0
	
	new make(MongoWriteCtx ppOpts) : super(ppOpts) { }

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
		if (size > ppOpts.ppMaxWidth) {
			// bufSize is only approx unless we start counting the lines in vals
			bufSize := (keys.size * maxKeySize) + (vals.size * maxValSize) + (keys.size * ppOpts.ppIndent.size * 2)
			json := StrBuf(bufSize)
			json.addChar(MongoValWriter.objectStart).addChar('\n')
			
			keys.each |key, i| {
				val := vals[i]
				
				json.add(ppOpts.ppIndent)
				json.add(key.justl(maxKeySize))
				json.addChar(MongoValWriter.colon)
				json.addChar(' ')
				
				lines := val.splitLines
				json.add(lines.first)
				if (lines.size > 1)
					lines.eachRange(1..-1) |line, j| {
						json.addChar('\n')
						json.add(ppOpts.ppIndent).add(line)
					}
				if (i < keys.size - 1)
					json.addChar(MongoValWriter.comma)
				json.addChar('\n')
			}
			
			json.addChar(MongoValWriter.objectEnd)
			return json.toStr

		} else {
			json := StrBuf(size)
			json.addChar(MongoValWriter.objectStart)
			keys.each |key, i| {
				val := vals[i]
				json.add(key).addChar(MongoValWriter.colon).addChar(' ').add(val)
				if (i < keys.size - 1)
					json.addChar(MongoValWriter.comma).addChar(' ')
			}
			json.addChar(MongoValWriter.objectEnd)
			return json.toStr
		}
	}
}

internal class MongoWriteCtxUgly : MongoWriteCtx {
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
	
	override This valueStart()		{ this										}
	override This valueEnd()		{ this										}
	
	override Void arrayStart()		{ out.writeChar(MongoValWriter.arrayStart)	}
	override Void arrayItem()		{ out.writeChar(MongoValWriter.comma)		}
	override Void arrayEnd()		{ out.writeChar(MongoValWriter.arrayEnd)	}

	override Void objectStart()		{ out.writeChar(MongoValWriter.objectStart)	}
	override Void objectKey()		{ out.writeChar(MongoValWriter.colon)		}
	override Void objectVal()		{ out.writeChar(MongoValWriter.comma)		}
	override Void objectEnd()		{ out.writeChar(MongoValWriter.objectEnd)	}

	override Void finalise()		{ 											}
}
