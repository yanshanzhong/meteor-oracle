Tinytest.add('test oracle modifier', function (test, done) {
	
	var s;

	//
	// Tests with numbers
	//
	s = OracleModifier.process({$inc: {age: 1}});
	test.equal(s, {".": "age = age + 1"});
	
	s = OracleModifier.process({$set: {age: 19}});
	test.equal(s, {".": "age = 19"});
	
	s = OracleModifier.process({$set: {name: "Amber", age: 37}});
	test.equal(s, {".": "name = 'Amber', age = 37"});
});
