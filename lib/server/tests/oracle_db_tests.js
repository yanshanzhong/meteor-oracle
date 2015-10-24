Tinytest.add(
  'OracleDB Test',
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

Tinytest.add(
		  'Name Converters Test',
		  function (test) {
			  var s;
			  
			  s = OracleDB._convertNameToCamelCase("AAA_BBB_CCC");
			  test.equal(s, "aaaBbbCcc");
			  
			  s = OracleDB._convertCamelCaseToName("aaaBbbCcc");
			  test.equal(s, "AAA_BBB_CCC");
		  }
		);
