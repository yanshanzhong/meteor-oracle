Tinytest.add(
  'create Oracle.Collection',
  function (test) {
	  	var options = {};
	  	
        var coll = new Oracle.Collection("hello", options);

        if(Meteor.isServer) {
    	  	var oracleOptions = {
    		  		connection: {
    		  			  user: "meteor", 
    		  			  password: "meteor", 
    		  			  connectString : "localhost/XE"
    		  		},
    				sql: "select table_name from user_tables where table_name like :1",
    				sqlParameters: ['DR%'],
    				sqlScn: null,
    				sqlAddId: true,
    				sqlDebug: false
    	  	};
    	  	
    	  	coll.setOracleOptions(oracleOptions);
        }
        
        test.isNotNull(coll);
        
        var rows = coll.find({}, {skip: 0, limit: 10, sort: {TABLE_NAME: 1}}).fetch();
        
        console.log(rows);
  }
);
