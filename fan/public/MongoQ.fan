using afBson::BsonIO

** A means to build common Mongo queries with sane objects and methods. (And not some incomprehensible mess of nested maps and lists!)
** 
** pre>
** syntax: fantom
** query := MongoQ().with |q| {
**     q.and(
**         q.or( q.eq("price", 0.99f), q.eq("price", 1.99f)  ),
**         q.or( q.eq("sale", true),   q.lessThan("qty", 29) )
**     )
** }.query
** <pre
class MongoQ {
	// this weird class is both the MongoQ AND its own builder!

	** The underlying query that's being build up.
	Str:Obj? query() { _innerQ?._query ?: _query }
	
	private Str:Obj? _query { private set }
	
	** Creates a standard MongoQ instance.
	new make() {
		this._query			= obj
		this._nameHookFn	= _defHookFn
		this._valueHookFn	= _defHookFn
	}
	
	** Create a query instance with name / value hooks.
	@NoDoc
	new makeWithHookFns(|Obj->Str| nameHookFn, |Obj?->Obj| valueHookFn) {
		this._query			= obj
		this._nameHookFn	= nameHookFn
		this._valueHookFn	= valueHookFn
	}	



	// ---- Comparison MongoQ Operators ------------------------------------------------------------
	
	** Matches values that are equal to the given object.
	** 
	**   syntax: fantom
	**   q.eq("score", 11)
	** 
	MongoQ eq(Obj name, Obj? value) {
		q.set(name, value)
	}
	
	** Matches values that are **not** equal to the given object.
	** 
	** Note this also matches documents that do not contain the field.
	** 
	**   syntax: fantom
	**   q.notEq("score", 11)
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/ne/`
	MongoQ notEq(Obj name, Obj? value) {
		q.op(name, "\$ne", value)
	}
	
	** Matches values that equal any one of the given values.
	** 
	**   syntax: fantom
	**   q.in("score", [9, 10, 11])
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/in/`
	MongoQ in(Obj name, Obj[] values) {
		q.op(name, "\$in", values)	// BSON converter is deep!
	}	

	** Matches values that do **not** equal any one of the given values.
	** 
	** Note this also matches documents that do not contain the field.
	** 
	**   syntax: fantom
	**   q.notIn("score", [1, 2, 3])
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/nin/`
	MongoQ notIn(Obj name, Obj[] values) {
		q.op(name, "\$nin", values)	// BSON converter is deep!
	}	
	
	** Matches values that are greater than the given object.
	** 
	**   syntax: fantom
	**   q.greaterThan("score", 8)
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/gt/`
	MongoQ greaterThan(Obj name, Obj value) {
		q.op(name, "\$gt", value)
	}
	
	** Matches values that are greater than or equal to the given object.
	** 
	**   syntax: fantom
	**   q.greaterThanOrEqTo("score", 8)
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/gte/`
	MongoQ greaterThanOrEqTo(Obj name, Obj value) {
		q.op(name, "\$gte", value)
	}	

	** Matches values that are less than the given object.
	** 
	**   syntax: fantom
	**   q.lessThan("score", 5)
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/gt/`
	MongoQ lessThan(Obj name, Obj value) {
		q.op(name, "\$lt", value)
	}

	** Matches values that are less than or equal to the given object.
	** 
	**   syntax: fantom
	**   q.lessThanOrEqTo("score", 5)
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/lte/`
	MongoQ lessThanOrEqTo(Obj name, Obj value) {
		q.op(name, "\$lte", value)
	}	
	

	
	// ---- Element MongoQ Operators ---------------------------------------------------------------

	** Matches if the field exists (or not), even if it is 'null'.
	** 
	**   syntax: fantom
	**   q.exists("score")
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/exists/`
	MongoQ exists(Obj name, Bool exists := true) {
		q.op(name, "\$exists", exists)
	}
	
	
	
	// ---- String MongoQ Operators ----------------------------------------------------------------
	
	** Matches string values that equal the given regular expression.
	** 
	**   syntax: fantom
	**   q.matchesRegex("name", "Emm?")
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/regex/`
	MongoQ matchesRegex(Obj name, Regex regex) {
		q.op(name, "\$regex", regex)
	}

	** Matches string values that equal (ignoring case) the given value.
	** Matching is performed with regular expressions. 
	** 
	**   syntax: fantom
	**   q.eqIgnoreCase("name", "emm?")
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/regex/`
	MongoQ eqIgnoreCase(Obj name, Str value) {
		matchesRegex(name, "(?i)^${Regex.quote(value)}\$".toRegex)
	}
	
	** Matches string values that contain the given value.
	** Matching is performed with regular expressions. 
	** 
	**   syntax: fantom
	**   q.contains("name", "Em")
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/regex/`
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
	** @see `http://docs.mongodb.org/manual/reference/operator/query/regex/`
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
	** @see `http://docs.mongodb.org/manual/reference/operator/query/regex/`
	MongoQ endsWith(Obj name, Str value, Bool caseInsensitive := true) {
		i := caseInsensitive ? "(?i)" : ""
		return matchesRegex(name, "${i}${Regex.quote(value)}\$".toRegex)
	}
	
	
	
	// ---- Evaluation MongoQ Operators ------------------------------------------------------------

	** Matches values based on their remainder after a division (modulo operation).
	** 
	**   syntax: fantom
	**   q.mod("score", 3, 0)
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/mod/`
	MongoQ mod(Obj name, Int divisor, Int remainder) {
		q.op(name, "\$mod", Int[divisor, remainder])	// BSON converter is deep!
	}
	
	
	
	// ---- Logical MongoQ Operators ---------------------------------------------------------------
	
	** Selects documents that do **not** match the given following criterion.
	** Example:
	** 
	**   syntax: fantom
	**   q.eq("score", 11).not
	**   q.not(q.eq("score", 11))
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/not/`
	MongoQ not(MongoQ query := this) {
query.dump
echo(query._query)
echo(query._innerQ)
		
//		a:=	q.set("\$not", query._query)
		query._query = obj["\$not"] = query._query
		query.dump
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
	** @see `http://docs.mongodb.org/manual/reference/operator/query/and/`
	MongoQ and(MongoQ q1, MongoQ q2, MongoQ? q3 := null, MongoQ? q4 := null) {
		qs := [q1._query, q2._query]
		if (q3 != null) qs.add(q3._query)
		if (q4 != null) qs.add(q4._query)
		return q.set("\$and", qs)
	}
	
	** Selects documents that pass any of the query expressions in the given list.
	** 
	**   syntax: fantom
	**   query := or(
	**     lessThan("quantity", 20),
	**     eq("price", 10)
	**   )
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/or/`
	MongoQ or(MongoQ q1, MongoQ q2, MongoQ? q3 := null, MongoQ? q4 := null) {
		qs := [q1._query, q2._query]
		if (q3 != null) qs.add(q3._query)
		if (q4 != null) qs.add(q4._query)
		return q.set("\$or", qs)
	}
	
	** Selects documents that fail **all** the query expressions in the given list.
	** 
	**   syntax: fantom
	**   query := nor(
	**     lessThan("quantity", 20),
	**     eq("price", 10)
	**   )
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/nor/`
	MongoQ nor(MongoQ q1, MongoQ q2, MongoQ? q3 := null, MongoQ? q4 := null) {
		qs := [q1._query, q2._query]
		if (q3 != null) qs.add(q3._query)
		if (q4 != null) qs.add(q4._query)
		return q.set("\$nor", qs)
	}	
	
	
	
	// ---- Text Operators ---------------------------------------------------------------
	
	** Performs a text search on the collection. 
	** 
	** Text searching makes use of stemming and ignores language stop words.
	** Quotes may be used to search for exact phrases and prefixing a word with a hyphen-minus (-) negates it.
	** 
	** Results are automatically ordered by search relevance.
	**  
	** To use text searching, make sure the Collection has a text Index else MongoDB will throw an Err.
	** 
	**   q.textSearch("some text")
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
		q.set("\$text", (obj["\$search"] = search).setAll(opts ?: obj))
		// FIXME auto-order by scoew	
//		((Str:Obj?) _orderBy)["_textScore"] = ["\$meta": "textScore"]
	}

	** Selects documents based on the return value of a javascript function. Example:
	** 
	**   syntax: fantom
	**   q.where("this.name == 'Judge Dredd'")
	** 
	** Only 1 *where* function is allowed per query.
	** 
	** @see `http://docs.mongodb.org/manual/reference/operator/query/where/`
	MongoQ where(Str where) {
		q.set("\$where", where)
	}
	
	

	// ---- Other ---------------------------------------------------------------
	
	Str print(Int maxWidth := 60, Str indent := "  ") {
		BsonIO().print(query, maxWidth, indent)
	}
	
	This dump(Int maxWidth := 60) {
		echo(print(maxWidth))
		return this
	}
	
	@NoDoc
	override Str toStr() { print(60) }

	** Returns the named query.
	@NoDoc
	@Operator
	Obj? get(Obj name) {
		_query.get(name)
	}
	
	** Sets the named query
	@NoDoc
	@Operator
	This set(Obj name, Obj? value) {
		_query.set(_nameHookFn(name), _valueHookFn(value))
		return this
	}
	
	** Sets an op.
	** 
	**   q.op("score", "\$neq", 11)  -->  q.set("score", ["\$neg", 11])
	@NoDoc
	This op(Obj name, Str op, Obj? value) {
		_query[_nameHookFn(name)] = (obj[op] = _valueHookFn(value))
		return this
	}
	
	// covert stuff *immediately* for instant err feedback
	private |Obj ->Str |		_nameHookFn
	private |Obj?->Obj?|		_valueHookFn
	private MongoQ?				_innerQ
	private static const Func	_defHookFn 	:= |Obj? v -> Obj?| { v }.toImmutable
	
	private MongoQ q() {
		// we can't check / throw this, 'cos we *may* be creating multiple instances for an AND or an OR filter.
		//if (_innerQ != null)	throw Err("Top level Mongo Query has already been set: ${toStr}")
		return _innerQ = MongoQ(_nameHookFn, _valueHookFn)
	}

	private Str:Obj? obj() {
		obj := Str:Obj?[:]
		obj.ordered = true
		return obj
	}
}
