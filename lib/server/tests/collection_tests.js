Tinytest.add(
  'create Oracle.Collection',
  function (test) {
	  	var options = {};
	  	
        var coll = new Oracle.Collection("tables", options);

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
        
        var rows = coll.find({}, {skip: 0, limit: 10, sort: {USERNAME: 1}}).fetch();
        
        test.equal(rows.length, 1);
        test.equal(rows[0].username, "METEOR");

        rows = coll.findOne({}, {skip: 0, limit: 10, sort: {USERNAME: 1}});

        test.equal(rows.username, "METEOR");
  }
);

Tinytest.add(
		  'create new collection hello',
		  function (test) {
			  	var options = {};
			  	
		        var hello = new Oracle.Collection("hello", options);

			  	// hello.setOracleOptions({booleanTrueValue: 1, booleanFalseValue: 0});
			  	hello.setOracleOptions({sqlDebug: true});
			  	
		        test.isNotNull(hello);

		        hello.remove({});
		        hello.insert({name: "John", age:37, city: "London", married: true, birthdate: Oracle.LocalDate("2010-05-19T00:00:00"),
		        		tags:[{tag:"CANCELLED", owner: "John"}, {tag: "SUBMITTED", owner: "Mary"}]});
		        hello.insert({name: "Annie", age:22, city: "New York", married: false, tags: [{tag:"OK", owner: "Helen" /*, groups: [{name: "ADMINS"}, {name: "USERS"}]*/}, {tag:"APPROVED", owner: "Keith"}, {tag: "QA PASSED", owner: "Curtis"}]});
		        hello.insert({name: "Jiang", age:28, city: "Beijing"});
		        
		        var rows = hello.findOne({age:22, ".tags": {tag: {$like: '%A%'}}}, {skip: 0, limit: 10, sort: {ID: 1}, fields: {age: true, city: true, tags: true, "tags.tag": true, "tags.owner": true}});
		        
		        console.log(rows);

		        rows = hello.findOne({}, {skip: 0, limit: 10, sort: {ID: 1}});
		        
		        console.log(rows);

		        rows = hello.findOne({'tags.0.tag': "CANCELLED"}, {skip: 0, limit: 10, sort: {ID: 1}});
		        
		        console.log(rows);
		        
		        rows = hello.findOne({"tags": {$elemMatch: {$or: [{tag: "OK", owner: "Helen"}, {tag: "CANCELLED"}]}}}, {skip: 0, limit: 10, sort: {ID: 1}});
		        
		        console.log(rows);
		  }
		);
