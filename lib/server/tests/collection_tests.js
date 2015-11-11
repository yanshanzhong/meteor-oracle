Tinytest.add(
  'Oracle.Collection custom select',
  function (test) {
	  	var options = {};
	  	
        var coll = new Oracle.Collection("users", options);

        if(Meteor.isServer) {
    	  	var oracleOptions = {
    				sql: "select * from user_users where username = :1",
    				sqlParameters: ['METEOR'],
    				sqlTable: null, // null means no DML, use undefined to apply the collection name as a default
    				sqlAddId: true
    	  	};
    	  	
    	  	coll.setOracleOptions(oracleOptions);
        }
        
        test.isNotNull(coll);
        
        var rows = coll.find({}, {skip: 0, limit: 10, sort: {USERNAME: 1}}).fetch();
        
        test.equal(rows.length, 1);
        test.equal(rows[0].USERNAME, "METEOR");

        rows = coll.findOne({}, {skip: 0, limit: 10, sort: {USERNAME: 1}});

        test.equal(rows.USERNAME, "METEOR");
  }
);

Tinytest.add(
		  'create new collection testNames',
		  function (test) {
			  	var options = {};
			  	
		        var testNames = new Oracle.Collection("testNames", options);
		        
		        test.isNotNull(testNames);
		        
		        // testNames.setOracleOptions({sqlDebug: true});

		        testNames._collection.dropCollection();
		        
		        var s = '## Welcome to Telescope!\n\nIf you\'re reading this, it means you\'ve successfully got Telescope to run.\n';

		        testNames.insert({name: s});

		        testNames._collection._ensureIndex({name: 1}, {unique:true, bitmap:false});
		        
		        var rows = testNames.find({}, {fields: {name: 1, _id: 0}}).fetch();
		        
		        test.equal(rows, [{"name": s}]);
		  }
		);

Tinytest.add(
		  'create new collection test_lists',
		  function (test) {
			  	var options = {};
			  	
		        var testLists = new Oracle.Collection("testLists", options);
		        
			  	// testLists.setOracleOptions({booleanTrueValue: 1, booleanFalseValue: 0});
			  	testLists.setOracleOptions({sqlDebug: true});
			  	
		        test.isNotNull(testLists);

		        testLists._collection.dropCollection();
		        
		        testLists.insert({name: "Principles", incompleteCount:37, userId: "Michael", closed: true, createdAt: Oracle.LocalDate("2010-05-19T00:00:00"),
		        		tags:[{tag:"CANCELLED", owner: "John"}, {tag: "SUBMITTED", owner: "Mary"}]});
		        testLists.insert({name: "Languages", incompleteCount:22, userId: "Andrea", closed: false, tags: [{tag:"OK", owner: "Helen" /*, groups: [{name: "ADMINS"}, {name: "USERS"}]*/}, {tag:"APPROVED", owner: "Keith"}, {tag: "QA PASSED", owner: "Curtis"}]});
		        testLists.insert({name: "Favorites", incompleteCount:28, userId: "Amit"});
		        
		        testLists._collection._ensureIndex({userId: 1, name: 1}, {unique:true, bitmap:false});
		        testLists._collection._ensureIndex({userId: 1, name: -1}, {unique:true, bitmap:false});
		        testLists._collection._ensureIndex({"tags._id": 1, "tags.tag": 1}, {unique:true, bitmap:false});
		        testLists._collection._ensureIndex({"tags._id": 1, "tags.tag": -1}, {unique:true, bitmap:false});
		        
		        testLists._collection._dropIndex({userId: 1, name: -1});
		        testLists._collection._dropIndex({"tags._id": 1, "tags.tag": -1});
		        
		        var rows = testLists.findOne({}, {skip: 0, limit: 10, sort: {_id: 1}});
		        
		        console.log(rows);

		        rows = testLists.findOne({incompleteCount:22, ".tags": {tag: {$like: '%A%'}}}, {skip: 0, limit: 10, sort: {_id: 1}, fields: {incompleteCount: true, userId: true, tags: true, "tags.tag": true, "tags.owner": true}});
		        
		        console.log(rows);

		        rows = testLists.findOne({'tags.0.tag': "CANCELLED"}, {skip: 0, limit: 10, sort: {_id: 1}});
		        
		        console.log(rows);
		        
		        rows = testLists.findOne({"tags": {$elemMatch: {$or: [{tag: "OK", owner: "Helen"}, {tag: "CANCELLED"}]}}}, {skip: 0, limit: 10, sort: {_id: 1}});
		        
		        console.log(rows);
		  }
		);

Tinytest.add(
		  'create new todo test_todos',
		  function (test) {
		        var testTodos = new Oracle.Collection("testTodos");
		        
		        test.isNotNull(testTodos);

		        testTodos._collection.dropCollection();
		        
		        // testTodos.insert({name: "Favorites", incompleteCount:28, userId: "Amit"});
		        
		        var rows = testTodos.find({}).fetch();
		        
		        console.log(rows);

		  }
		);
