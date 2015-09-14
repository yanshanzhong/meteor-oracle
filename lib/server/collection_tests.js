Tinytest.add(
  'create Oracle.Collection',
  function (test) {
	  	var options = {
	  		  user: "meteor", 
			  password: "meteor", 
			  connectString : "localhost/XE"
	  	};
	  	
	  	options.sql = "select table_name from user_tables where table_name like :1";
	  	options.sqlParameters = ['DR%'];
	  	
        var coll = new Oracle.Collection("hello", options);
        
        test.isNotNull(coll);
        
        var rows = coll.find({}, {skip: 0, limit: 4, sort: {AGE: -1}}).fetch();
        
        console.log(rows);
     
        coll.close();
  }
);
