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
    				sql: "select table_name from user_tables where table_name like :1",
    				sqlParameters: ['DR%'],
    				sqlTable: null, // null means no DML, use undefined to apply the collection name as a default
    				sqlScn: null,
    				sqlAddId: true,
    				sqlDebug: false
    	  	};
    	  	
    	  	coll.setOracleOptions(oracleOptions);
        }
        
        test.isNotNull(coll);
        
        var rows = coll.find({}, {skip: 0, limit: 10, sort: {TABLE_NAME: 1}}).fetch();
        
        console.log(rows);

        rows = coll.findOne({}, {skip: 0, limit: 10, sort: {TABLE_NAME: 1}});
        
        console.log(rows);
  }
);

Tinytest.add(
		  'create new collection hello',
		  function (test) {
			  	var options = {};
			  	
		        var hello = new Oracle.Collection("hello", options);

		        test.isNotNull(hello);
		        
		        hello.insert({name: "John", age:37, city: "London"});
		        hello.insert({name: "Annie", age:22, city: "New York"});
		        hello.insert({name: "Jiang", age:25, city: "Beijing"});
		        
		        var rows = hello.find({}, {skip: 0, limit: 10, sort: {ID: 1}}).fetch();
		        
		        console.log(rows);

		        rows = hello.findOne({}, {skip: 0, limit: 10, sort: {ID: 1}});
		        
		        console.log(rows);
		  }
		);
