Tinytest.add(
  'create Oracle.Collection',
  function (test) {
	  	var options = {};
	  	
        var coll = new Oracle.Collection("users", options);

        if(Meteor.isServer) {
    	  	var oracleOptions = {
    		  		connection: {
    		  			  user: "meteor", 
    		  			  password: "meteor", 
    		  			  connectString : "localhost/XE"
    		  		},
    				sql: "select * from user_users where username = :1",
    				sqlParameters: ['METEOR'],
    				sqlTable: null, // null means no DML, use undefined to apply the collection name as a default
    				sqlScn: null,
    				sqlAddId: true,
    				sqlDebug: false
    	  	};
    	  	
    	  	coll.setOracleOptions(oracleOptions);
        }
        
        test.isNotNull(coll);
        
        var rows = coll.find({}, {skip: 0, limit: 10, sort: {username: 1}}).fetch();
        
        test.equal(rows.length, 1);
        test.equal(rows[0].username, "METEOR");

        rows = coll.findOne({}, {skip: 0, limit: 10, sort: {username: 1}});

        test.equal(rows.username, "METEOR");
  }
);

Tinytest.add(
		  'create new collection hello',
		  function (test) {
			  	var options = {};
			  	
		        var testLists = new Oracle.Collection("test_lists", options);
		        
			  	// testLists.setOracleOptions({booleanTrueValue: 1, booleanFalseValue: 0});
			  	testLists.setOracleOptions({sqlDebug: true});
			  	
		        test.isNotNull(testLists);

		        testLists._collection.dropCollection();
		        
		        testLists.insert({name: "Principles", incompleteCount:37, userId: "Michael", closed: true, createdAt: Oracle.LocalDate("2010-05-19T00:00:00"),
		        		testTags:[{tag:"CANCELLED", owner: "John"}, {tag: "SUBMITTED", owner: "Mary"}]});
		        testLists.insert({name: "Languages", incompleteCount:22, userId: "Andrea", closed: false, testTags: [{tag:"OK", owner: "Helen" /*, groups: [{name: "ADMINS"}, {name: "USERS"}]*/}, {tag:"APPROVED", owner: "Keith"}, {tag: "QA PASSED", owner: "Curtis"}]});
		        testLists.insert({name: "Favorites", incompleteCount:28, userId: "Amit"});
		        
		        testLists._collection._ensureIndex({name: 1}, {unique:false, bitmap:false});
		        
		        var rows = testLists.findOne({incompleteCount:22, ".testTags": {tag: {$like: '%A%'}}}, {skip: 0, limit: 10, sort: {_id: 1}, fields: {incompleteCount: true, userId: true, testTags: true, "testTags.tag": true, "testTags.owner": true}});
		        
		        console.log(rows);

		        rows = testLists.findOne({}, {skip: 0, limit: 10, sort: {_id: 1}});
		        
		        console.log(rows);

		        rows = testLists.findOne({'testTags.0.tag': "CANCELLED"}, {skip: 0, limit: 10, sort: {_id: 1}});
		        
		        console.log(rows);
		        
		        rows = testLists.findOne({"testTags": {$elemMatch: {$or: [{tag: "OK", owner: "Helen"}, {tag: "CANCELLED"}]}}}, {skip: 0, limit: 10, sort: {_id: 1}});
		        
		        console.log(rows);
		  }
		);

Tinytest.add(
		  'create new todo hello',
		  function (test) {
		        var testTodos = new Oracle.Collection("test_todos");
		        
		        test.isNotNull(testTodos);

		        testTodos._collection.dropCollection();
		        
		        // testTodos.insert({name: "Favorites", incompleteCount:28, userId: "Amit"});
		        
		        var rows = testTodos.find({});
		        
		        console.log(rows);

		  }
		);
