using afBson::BsonIO

** A means to build common Mongo queries with sane objects and methods.
** (And not some incomprehensible mess of nested maps and lists!)
** 
** pre>
** syntax: fantom
** query := MongoQ {
**     and(
**         or( eq("price", 0.99f), eq("price", 1.99f)  ),
**         or( eq("sale", true),   lessThan("qty", 29) )
**     )
** }.query
** <pre
class MongoQ {
	// this weird class is both the MongoQ AND its own builder!

	** The underlying query that's being build up.
	Str:Obj? query() {
		if (_innerQ != null)
			return obj[_innerQ._key] = _innerQ._val
		if (_key != null)
			return obj[_key] = _val
		// if _key is null, then NO query methods have been called - we're empty!
		return obj
	}
	
	private Str?	_key
	private Obj?	_val
	private Bool	_not
	
	** Creates a standard MongoQ instance.
	new make() {
		this._nameHookFn	= _defHookFn
		this._valueHookFn	= _defHookFn
	}
	
	** Create a query instance with name / value hooks.
	@NoDoc
	new makeWithHookFns(|Obj->Str| nameHookFn, |Obj?->Obj?| valueHookFn) {
		this._nameHookFn	= nameHookFn
		this._valueHookFn	= valueHookFn
	}	



	// ---- Comparison MongoQ Operators ---------
	
	** Matches values that are equal to the given object.
	** 
	**   syntax: fantom
	**   q.eq("score", 11)
	** 
	** Shorthand notation.
	** 
	**   syntax: fantom
	**   q->score = 11
	** 
	MongoQ eq(Obj name, Obj? value) {
		_q._set(name, value)
	}
	
	** Matches values that are **not** equal to the given object.
	** 
	** Note this also matches documents that do not contain the field.
	** 
	**   syntax: fantom
	**   q.notEq("score", 11)
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/ne/`
	MongoQ notEq(Obj name, Obj? value) {
		_q.op(name, "\$ne", value)
	}
	
	** Matches values that equal any one of the given values.
	** 
	**   syntax: fantom
	**   q.in("score", [9, 10, 11])
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/in/`
	MongoQ in(Obj name, Obj[] values) {
		_q.op(name, "\$in", values)	// BSON converter is deep!
	}	

	** Matches values that do **not** equal any one of the given values.
	** 
	** Note this also matches documents that do not contain the field.
	** 
	**   syntax: fantom
	**   q.notIn("score", [1, 2, 3])
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/nin/`
	MongoQ notIn(Obj name, Obj[] values) {
		_q.op(name, "\$nin", values)	// BSON converter is deep!
	}	
	
	** Matches values that are greater than the given object.
	** 
	**   syntax: fantom
	**   q.greaterThan("score", 8)
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/gt/`
	MongoQ greaterThan(Obj name, Obj value) {
		_q.op(name, "\$gt", value)
	}
	
	** Matches values that are greater than or equal to the given object.
	** 
	**   syntax: fantom
	**   q.greaterThanOrEqTo("score", 8)
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/gte/`
	MongoQ greaterThanOrEqTo(Obj name, Obj value) {
		_q.op(name, "\$gte", value)
	}	

	** Matches values that are less than the given object.
	** 
	**   syntax: fantom
	**   q.lessThan("score", 5)
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/gt/`
	MongoQ lessThan(Obj name, Obj value) {
		_q.op(name, "\$lt", value)
	}

	** Matches values that are less than or equal to the given object.
	** 
	**   syntax: fantom
	**   q.lessThanOrEqTo("score", 5)
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/lte/`
	MongoQ lessThanOrEqTo(Obj name, Obj value) {
		_q.op(name, "\$lte", value)
	}	
	

	
	// ---- Element MongoQ Operators ------------

	** Matches if the field exists (or not), even if it is 'null'.
	** 
	**   syntax: fantom
	**   q.exists("score")
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/exists/`
	MongoQ exists(Obj name, Bool exists := true) {
		_q.op(name, "\$exists", exists)
	}
	
	
	
	// ---- String MongoQ Operators -------------
	
	** Matches string values that equal the given regular expression.
	** 
	**   syntax: fantom
	**   q.matchesRegex("name", "Emm?")
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/regex/`
	MongoQ matchesRegex(Obj name, Regex regex) {
		_q.op(name, "\$regex", regex)
	}

	** Matches string values that equal (ignoring case) the given value.
	** Matching is performed with regular expressions. 
	** 
	**   syntax: fantom
	**   q.eqIgnoreCase("name", "emm?")
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/regex/`
	MongoQ eqIgnoreCase(Obj name, Str value) {
		matchesRegex(name, "(?i)^${Regex.quote(value)}\$".toRegex)
	}
	
	** Matches string values that contain the given value.
	** Matching is performed with regular expressions. 
	** 
	**   syntax: fantom
	**   q.contains("name", "Em")
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/regex/`
	MongoQ contains(Obj name, Str value, Bool caseInsensitive := true) {
		i := caseInsensitive ? "(?i)" : ""
		return matchesRegex(name, "${i}${Regex.quote(value)}".toRegex)
	}

	** Matches string values that start with the given value.
	** Matching is performed with regular expressions. 
	** 
	**   syntax: fantom
	**   q.startsWith("name", "Em")
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/regex/`
	MongoQ startsWith(Obj name, Str value, Bool caseInsensitive := true) {
		i := caseInsensitive ? "(?i)" : ""
		return matchesRegex(name, "${i}^${Regex.quote(value)}".toRegex)
	}

	** Matches string values that end with the given value.
	** Matching is performed with regular expressions. 
	** 
	**   syntax: fantom
	**   q.endsWith("name", "ma")
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/regex/`
	MongoQ endsWith(Obj name, Str value, Bool caseInsensitive := true) {
		i := caseInsensitive ? "(?i)" : ""
		return matchesRegex(name, "${i}${Regex.quote(value)}\$".toRegex)
	}
	
	
	
	// ---- Evaluation MongoQ Operators ---------

	** Matches values based on their remainder after a division (modulo operation).
	** 
	**   syntax: fantom
	**   q.mod("score", 3, 0)
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/mod/`
	MongoQ mod(Obj name, Int divisor, Int remainder) {
		_q.op(name, "\$mod", Int[divisor, remainder])	// BSON converter is deep!
	}
	
	
	
	// ---- Logical MongoQ Operators ------------
	
	** Selects documents that do **not** match the given following criterion.
	** Example:
	** 
	**   syntax: fantom
	**   not.eq("score", 11)
	**   eq("score", 11).not
	**   not(q.eq("score", 11))
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/not/`
	MongoQ not(MongoQ query := this) {
		if (query._val != null)
			query._val = (obj["\$not"] = query._val)
		else
			query._not = true
		return query
	}	
	
	** Selects documents that pass all the query expressions in the given list.
	** 
	**   syntax: fantom
	**   q.and(
	**     q.lessThan("quantity", 20),
	**     q.eq("price", 10)
	**   )
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/and/`
	MongoQ and(MongoQ q1, MongoQ q2, MongoQ? q3 := null, MongoQ? q4 := null) {
		qs := [q1._query, q2._query]
		if (q3 != null) qs.add(q3._query)
		if (q4 != null) qs.add(q4._query)
		return _q._set("\$and", qs)
	}
	
	** Selects documents that pass any of the query expressions in the given list.
	** 
	**   syntax: fantom
	**   query := or(
	**     lessThan("quantity", 20),
	**     eq("price", 10)
	**   )
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/or/`
	MongoQ or(MongoQ q1, MongoQ q2, MongoQ? q3 := null, MongoQ? q4 := null) {
		qs := [q1._query, q2._query]
		if (q3 != null) qs.add(q3._query)
		if (q4 != null) qs.add(q4._query)
		return _q._set("\$or", qs)
	}
	
	** Selects documents that fail **all** the query expressions in the given list.
	** 
	**   syntax: fantom
	**   query := nor(
	**     lessThan("quantity", 20),
	**     eq("price", 10)
	**   )
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/nor/`
	MongoQ nor(MongoQ q1, MongoQ q2, MongoQ? q3 := null, MongoQ? q4 := null) {
		qs := [q1._query, q2._query]
		if (q3 != null) qs.add(q3._query)
		if (q4 != null) qs.add(q4._query)
		return _q._set("\$nor", qs)
	}	
	
	
	
	// ---- Text Operators ----------------------
	
	** Performs a text search on the collection. 
	** 
	** Text searching makes use of stemming and ignores language stop words.
	** Quotes may be used to search for exact phrases and prefixing a word with a hyphen-minus (-) negates it.
	** 
	** To enable text searching, ensure the Collection has a text Index else MongoDB will throw an Err.
	** 
	** To sort by search relevance, add the following projection AND sort.
	** 
	**   syntax: fantom
	**   col.find(MongoQ { textSearch("quack") }) {
	**     it->projection = ["_textScore": ["\$meta": "textScore"]]
	**     it->sort       = ["_textScore": ["\$meta": "textScore"]]
	**   }
	** 
	** 'options' may include the following:
	** 
	**   table:
	**   Name                 Type  Desc
	**   ----                 ----  ----                                              
	**   $language            Bool  Determines the list of stop words for the search and the rules for the stemmer and tokenizer. See [Supported Text Search Languages]`https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages`. Specify 'none' for simple tokenization with no stop words and no stemming. Defaults to the language of the index.
	**   $caseSensitive       Bool  Enable or disable case sensitive searching. Defaults to 'false'.
	**   $diacriticSensitive  Int   Enable or disable diacritic sensitive searching. Defaults to 'false'.
	** 
	** @see `https://docs.mongodb.com/manual/reference/operator/query/text/`.
	MongoQ textSearch(Str search, [Str:Obj?]? opts := null) {
		_q := _q
		_q._key = "\$text"
		_q._val = (obj["\$search"] = search).setAll(opts ?: obj)
		return _q
	}

	** Selects documents based on the return value of a javascript function. Example:
	** 
	**   syntax: fantom
	**   q.where("this.name == 'Judge Dredd'")
	** 
	** Only 1 *where* function is allowed per query.
	** 
	** @see `https://www.mongodb.com/docs/manual/reference/operator/query/where/`
	MongoQ where(Str where) {
		_q._set("\$where", where)
	}
	
	

	// ---- Other -------------------------------
	
	Str print(Int maxWidth := 60, Str indent := "  ") {
		BsonIO().print(query, maxWidth, indent)
	}
	
	This dump(Int maxWidth := 60) {
		echo(print(maxWidth))
		return this
	}
	
	@NoDoc
	override Str toStr() { print(60) }

	** it->field = value
	@NoDoc
	override Obj? trap(Str name, Obj?[]? args := null) {
		eq(name, args?.first)
	}
	
	private This _set(Obj name, Obj? value) {
		_key = _nameHookFn(name)
		_val = _valueHookFn(value)
		if (_not) {
			not
			_not = false
		}
		return this
	}
	
	** Sets an op.
	** 
	**   op("score", "\$neq", 11)  -->  set("score", ["\$neg", 11])
	@NoDoc
	This op(Obj name, Str op, Obj? value) {
		_key = _nameHookFn(name)
		_val = (obj[op] = _valueHookFn(value))
		if (_not) {
			not
			_not = false
		}
		return this
	}
	
	// covert stuff *immediately* for instant err feedback
	private |Obj ->Str |		_nameHookFn
	private |Obj?->Obj?|		_valueHookFn
	private MongoQ?				_innerQ
	private Str:Obj				_query() 	{ obj[_key] = _val }
	private static const Func	_defHookFn 	:= |Obj? v -> Obj?| { v }.toImmutable
	private MongoQ _q() {
		// we can't check / throw this, 'cos we *may* be creating multiple instances for an AND or an OR filter.
		//if (_innerQ != null)	throw Err("Top level Mongo Query has already been set: ${toStr}")
		q := _innerQ = MongoQ(_nameHookFn, _valueHookFn)
		q._not = _not
		_not = false
		return q
	}

	private Str:Obj? obj() {
		obj := Str:Obj?[:]
		obj.ordered = true
		return obj
	}
}
