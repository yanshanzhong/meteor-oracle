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

Tinytest.add(
		  'Name Converters',
		  function (test) {
			  var s;
			  
			  s = OracleDB._convertNameToCamelCase("AAA_BBB_CCC");
			  test.equal(s, "aaaBbbCcc");
			  
			  s = OracleDB._convertCamelCaseToName("aaaBbbCcc");
			  test.equal(s, "aaa_bbb_ccc");
			  
			  s = OracleDB._tableNameO2M("aaa_bbb_ccc");
			  test.equal(s, "aaaBbbCcc");
			  
			  s = OracleDB._tableNameM2O("aaaBbbCcc");
			  test.equal(s, "aaa_bbb_ccc");

			  s = OracleDB._columnNameO2M("aaa_bbb_ccc");
			  test.equal(s, "aaaBbbCcc");
			  
			  s = OracleDB._columnNameM2O("aaaBbbCcc");
			  test.equal(s, "aaa_bbb_ccc");

			  s = OracleDB._columnNameO2M("id");
			  test.equal(s, "_id");
			  
			  s = OracleDB._columnNameM2O("_id");
			  test.equal(s, "id");
		  }
		);
