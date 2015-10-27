Tinytest.add('test oracle fields _prepare', function (test, done) {
	
	var s;

	//
	// Tests with numbers
	//
	s = OracleFields._prepare({name: true, age: true});
	test.equal(s, {".":{"name":true,"age":true}});
	
	s = OracleFields._prepare({name: false, age:false});
	test.equal(s, {".":{"name":false,"age":false}});
	
	s = OracleFields._prepare({name: false, age:false, "tags.name": true, "tags.owner": true});
	test.equal(s, {".":{"name":false,"age":false,"owner":true},"tags":{".":{"name":true}}});
	
});

Tinytest.add('test oracle fields getColumnList', function (test, done) {
	
	var s;

	//
	// Tests with numbers
	//
	s = OracleFields._prepare({name: true, age: true});
	test.equal(s, {".":{"name":true,"age":true}});
	
	s = OracleFields._prepare({name: false, age:false});
	test.equal(s, {".":{"name":false,"age":false}});
	
	s = OracleFields._prepare({name: false, age:false, "tags.name": true, "tags.owner": true});
	test.equal(s, {".":{"name":false,"age":false,"owner":true},"tags":{".":{"name":true}}});
	
});