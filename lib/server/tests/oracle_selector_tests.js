
Tinytest.add('test oracle selector', function (test, done) {
	
	var s;

	//
	// Tests with numbers
	//
	s = new OracleSelector({_id: 17}, []);
	test.equal(s._docMatcher, "_id = 17");
	
	s = new OracleSelector({DONE: {$gt: 17}}, []);
	test.equal(s._docMatcher, "DONE > 17");
	
	s = new OracleSelector({DONE: {$lt: 17}}, []);
	test.equal(s._docMatcher, "DONE < 17");
	
	s = new OracleSelector({DONE: {$gte: 17}}, []);
	test.equal(s._docMatcher, "DONE >= 17");
	
	s = new OracleSelector({DONE: {$lte: 17}}, []);
	test.equal(s._docMatcher, "DONE <= 17");
	
	s = new OracleSelector({DONE: {$mod: [5, 3]}}, []);
	test.equal(s._docMatcher, "mod(DONE, 5) = 3");

	s = new OracleSelector({DONE: {$gte: 17}, AGE:{$mod: [5, 3]}}, []);
	test.equal(s._docMatcher, "(DONE >= 17) AND (mod(AGE, 5) = 3)");

	s = new OracleSelector({DONE: {$in: [17, 13, 35, 198]}}, []);
	test.equal(s._docMatcher, "DONE IN (17, 13, 35, 198)");

	s = new OracleSelector({DONE: {$nin: [17, 13, 35, 198]}}, []);
	test.equal(s._docMatcher, "DONE NOT IN (17, 13, 35, 198)");

	s = new OracleSelector({DONE: 17}, []);
	test.equal(s._docMatcher, "DONE = 17");
	
	s = new OracleSelector({DONE: {$eq: 17}}, []);
	test.equal(s._docMatcher, "DONE = 17");
	
	s = new OracleSelector({DONE: {$ne: 17}}, []);
	test.equal(s._docMatcher, "(DONE <> 17 OR DONE IS NULL)");
	
	s = new OracleSelector({DONE: {$not: 17}}, []);
	test.equal(s._docMatcher, "NOT(DONE = 17)");
	
	s = new OracleSelector({$and: [{DONE: {$gte: 17}}, {AGE: {$lte: 65}}]}, []);
	test.equal(s._docMatcher, "(DONE >= 17) AND (AGE <= 65)");

	s = new OracleSelector({$or: [{DONE: {$gte: 17}}, {AGE: {$lte: 65}}]}, []);
	test.equal(s._docMatcher, "(DONE >= 17) OR (AGE <= 65)");

	s = new OracleSelector({$nor: [{DONE: {$gte: 17}}, {AGE: {$lte: 65}}]}, []);
	test.equal(s._docMatcher, "NOT((DONE >= 17) OR (AGE <= 65))");

	//
	// Tests with Strings
	//
	s = new OracleSelector({DONE: {$lte: "Hello"}}, []);
	test.equal(s._docMatcher, "DONE <= 'Hello'");
	
	s = new OracleSelector({DONE: {$in: ["Hello", "World"]}}, []);
	test.equal(s._docMatcher, "DONE IN ('Hello', 'World')");

	s = new OracleSelector({DONE: {$nin: ["Hello", "World"]}}, []);
	test.equal(s._docMatcher, "DONE NOT IN ('Hello', 'World')");

	s = new OracleSelector({DONE: "Hello"}, []);
	test.equal(s._docMatcher, "DONE = 'Hello'");

	s = new OracleSelector({DONE: {$eq: "Hello"}}, []);
	test.equal(s._docMatcher, "DONE = 'Hello'");

	s = new OracleSelector({DONE: {$ne: "Hello"}}, []);
	test.equal(s._docMatcher, "(DONE <> 'Hello' OR DONE IS NULL)");

	s = new OracleSelector({DONE: {$not: "Hello"}}, []);
	test.equal(s._docMatcher, "NOT(DONE = 'Hello')");
	
	s = new OracleSelector({DONE: {$regex: "Hello*"}}, []);
	test.equal(s._docMatcher, "REGEXP_LIKE(DONE, 'Hello*')");

	s = new OracleSelector({$and: [{DONE: {$gte: "Hello"}}, {AGE: {$lte: "Worl'd"}}]}, []);
	test.equal(s._docMatcher, "(DONE >= 'Hello') AND (AGE <= 'Worl''d')");

	s = new OracleSelector({$or: [{DONE: {$gte: "Hello"}}, {AGE: {$lte: "Worl'd"}}]}, []);
	test.equal(s._docMatcher, "(DONE >= 'Hello') OR (AGE <= 'Worl''d')");

	s = new OracleSelector({$nor: [{DONE: {$gte: "Hello"}}, {AGE: {$lte: "Worl'd"}}]}, []);
	test.equal(s._docMatcher, "NOT((DONE >= 'Hello') OR (AGE <= 'Worl''d'))");

	s = new OracleSelector({DONE: {$lte: "Hello"}, $comment: "This is a comment"}, []);
	test.equal(s._docMatcher, "DONE <= 'Hello'");
	
	s = new OracleSelector({$where: "substr(NAME, 1, 3) = 'Jan'", $comment: "This is a comment"}, []);
	test.equal(s._docMatcher, "substr(NAME, 1, 3) = 'Jan'");
	
	//
	// Tests with Boolean
	//
	
	s = new OracleSelector({DONE: {$exists: true}}, []);
	test.equal(s._docMatcher, "DONE IS NOT NULL");

	s = new OracleSelector({DONE: {$exists: false}}, []);
	test.equal(s._docMatcher, "DONE IS NULL");

	//
	// Test with empty
	//
	s = new OracleSelector({}, []);
	test.equal(s._docMatcher, "");
	
	s = new OracleSelector({"tags.0.tag": {$eq: "OK"}});
	test.equal(s._docMatcher, "id IN (SELECT DISTINCT id FROM tags WHERE index_no = 0 AND tag = 'OK')");
	
	s = new OracleSelector({"tags": {$elemMatch: {tag: "OK", owner: "Helen"}}});
	test.equal(s._docMatcher, "id IN (SELECT DISTINCT id FROM tags WHERE (tag = 'OK') AND (owner = 'Helen'))");
	
	//
	// Custom extensions tests
	//
	s = new OracleSelector({DONE: {$like: "Hello%"}}, []);
	test.equal(s._docMatcher, "DONE LIKE 'Hello%'");

});

