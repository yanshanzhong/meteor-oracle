
Tinytest.add('test oracle selector', function (test, done) {
	
	var s;

	//
	// Tests with numbers
	//
	s = new OracleSelector({_id: 17}, []);
	test.equal(s._docMatcher, "id = 17");
	
	s = new OracleSelector({age: {$gt: 17}}, []);
	test.equal(s._docMatcher, "age > 17");
	
	s = new OracleSelector({age: {$lt: 17}}, []);
	test.equal(s._docMatcher, "age < 17");
	
	s = new OracleSelector({age: {$gte: 17}}, []);
	test.equal(s._docMatcher, "age >= 17");
	
	s = new OracleSelector({age: {$lte: 17}}, []);
	test.equal(s._docMatcher, "age <= 17");
	
	s = new OracleSelector({age: {$mod: [5, 3]}}, []);
	test.equal(s._docMatcher, "mod(age, 5) = 3");

	s = new OracleSelector({cnt: {$gte: 17}, age:{$mod: [5, 3]}}, []);
	test.equal(s._docMatcher, "(cnt >= 17) AND (mod(age, 5) = 3)");

	s = new OracleSelector({age: {$in: [17, 13, 35, 198]}}, []);
	test.equal(s._docMatcher, "age IN (17, 13, 35, 198)");

	s = new OracleSelector({age: {$nin: [17, 13, 35, 198]}}, []);
	test.equal(s._docMatcher, "age NOT IN (17, 13, 35, 198)");

	s = new OracleSelector({age: 17}, []);
	test.equal(s._docMatcher, "age = 17");
	
	s = new OracleSelector({age: {$eq: 17}}, []);
	test.equal(s._docMatcher, "age = 17");
	
	s = new OracleSelector({age: {$ne: 17}}, []);
	test.equal(s._docMatcher, "(age <> 17 OR age IS NULL)");
	
	s = new OracleSelector({age: {$not: 17}}, []);
	test.equal(s._docMatcher, "NOT(age = 17)");
	
	s = new OracleSelector({$and: [{age: {$gte: 17}}, {age: {$lte: 65}}]}, []);
	test.equal(s._docMatcher, "(age >= 17) AND (age <= 65)");

	s = new OracleSelector({$or: [{age: {$gte: 17}}, {age: {$lte: 65}}]}, []);
	test.equal(s._docMatcher, "(age >= 17) OR (age <= 65)");

	s = new OracleSelector({$nor: [{age: {$gte: 17}}, {age: {$lte: 65}}]}, []);
	test.equal(s._docMatcher, "NOT((age >= 17) OR (age <= 65))");

	//
	// Tests with Strings
	//
	s = new OracleSelector({name: {$lte: "Bill"}}, []);
	test.equal(s._docMatcher, "name <= 'Bill'");
	
	s = new OracleSelector({name: {$in: ["Bill", "Jane"]}}, []);
	test.equal(s._docMatcher, "name IN ('Bill', 'Jane')");

	s = new OracleSelector({name: {$nin: ["Bill", "Jane"]}}, []);
	test.equal(s._docMatcher, "name NOT IN ('Bill', 'Jane')");

	s = new OracleSelector({name: "Bill"}, []);
	test.equal(s._docMatcher, "name = 'Bill'");

	s = new OracleSelector({name: {$eq: "Bill"}}, []);
	test.equal(s._docMatcher, "name = 'Bill'");

	s = new OracleSelector({name: {$ne: "Bill"}}, []);
	test.equal(s._docMatcher, "(name <> 'Bill' OR name IS NULL)");

	s = new OracleSelector({name: {$not: "Bill"}}, []);
	test.equal(s._docMatcher, "NOT(name = 'Bill')");
	
	s = new OracleSelector({name: {$regex: "Bill*"}}, []);
	test.equal(s._docMatcher, "REGEXP_LIKE(name, 'Bill*')");

	s = new OracleSelector({$and: [{name: {$gte: "Bill"}}, {age: {$lte: "Jane's"}}]}, []);
	test.equal(s._docMatcher, "(name >= 'Bill') AND (age <= 'Jane''s')");

	s = new OracleSelector({$or: [{name: {$gte: "Bill"}}, {age: {$lte: "Jane's"}}]}, []);
	test.equal(s._docMatcher, "(name >= 'Bill') OR (age <= 'Jane''s')");

	s = new OracleSelector({$nor: [{name: {$gte: "Bill"}}, {age: {$lte: "Jane's"}}]}, []);
	test.equal(s._docMatcher, "NOT((name >= 'Bill') OR (age <= 'Jane''s'))");

	s = new OracleSelector({name: {$lte: "Bill"}, $comment: "This is a comment"}, []);
	test.equal(s._docMatcher, "name <= 'Bill'");
	
	s = new OracleSelector({$where: "substr(name, 1, 3) = 'Bil'", $comment: "This is a comment"}, []);
	test.equal(s._docMatcher, "substr(name, 1, 3) = 'Bil'");
	
	//
	// Tests with Boolean
	//
	
	s = new OracleSelector({name: {$exists: true}}, []);
	test.equal(s._docMatcher, "name IS NOT NULL");

	s = new OracleSelector({name: {$exists: false}}, []);
	test.equal(s._docMatcher, "name IS NULL");

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
	s = new OracleSelector({name: {$like: "Bill%"}}, []);
	test.equal(s._docMatcher, "name LIKE 'Bill%'");

});

