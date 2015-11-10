Tinytest.add(
  'OracleDB',
  function (test) {
	  var connection = {
    		  			  user: "meteor", 
    		  			  password: "meteor", 
    		  			  connectString : "localhost/XE"
    		  		};

	  var db = OracleDB.getDatabase(connection);
	  
	  test.isNotNull(db);
	  
	  var result = OracleDB.executeCommand(connection, "select * from user_users", [], {});

	  test.isNotNull(result);
	  test.isNotNull(result.rows);
	  test.isNotNull(result.metaData);	  
  }
);


