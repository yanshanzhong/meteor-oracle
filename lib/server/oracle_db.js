var path = Npm.require('path');
var Fiber = Npm.require('fibers');
var Future = Npm.require(path.join('fibers', 'future'));

OracleDB = function (connection) {
	  var self = this;
	  
	  self.connection = connection;
	  self.db = NpmModuleOracledb;
	  self.db.maxRows = 1000;
	  self.db.prefetchRows = 100;
	  self.db.autoCommit = true;
	  self._tables = {};
};

OracleDB._databases = {};

OracleDB.getDatabase = function(connection) {
	var key = connection.user+"@"+connection.connectString;
	var db = OracleDB._databases[key];
	
	if(!db) {
		var user = OracleDB.executeCommand(connection, "select * from user_users", [], {});
		
		db = new OracleDB(connection);
		db.user = user;
		
		OracleDB._databases[key] = db;
	}

	return db;
};

OracleDB._convertNameToCamelCase = function(name) {
	  var name2 = "";
	  var nextFlip = false;
	  for(var ni in name) {
		  var c = name[ni];
		  if(c === '_') {
			  nextFlip = true;
			  continue;
		  } else if(!nextFlip && 'A' <= c && c <= 'Z') {
			  c = c.toLowerCase();
		  } else if(nextFlip && 'a' <= c && c <= 'z') {
			  c = c.toUpperCase();
		  }
		  name2 = name2 + c;
		  nextFlip = false;
	  }	
	  
	  return name2;
};

OracleDB._convertCamelCaseToName = function(name) {
	  var name2 = "";
	  for(var ni in name) {
		  var c = name[ni];
		  if('A' <= c && c <= 'Z') {
			  name2 = name2 + "_";
		  } else if('a' <= c && c <= 'z') {
			  c = c.toUpperCase();
		  }
		  name2 = name2 + c;
	  }	
	  
	  return name2;
};

// Meteor -> Oracle
OracleDB._tableNameM2O = function(name) {
	return OracleDB._convertCamelCaseToName(name);
};

OracleDB._columnNameM2O = function(name) {
	if(name === "_id") {
		return "ID";
	}
	
	return OracleDB._convertCamelCaseToName(name);
};

//Oracle -> Meteor
OracleDB._tableNameO2M = function(name) {
	return OracleDB._convertNameToCamelCase(name);
};

OracleDB._columnNameO2M = function(name) {
	if(name === "ID") {
		return "_id";
	}
	
	return OracleDB._convertNameToCamelCase(name);
};


OracleDB._processResult = function(sqlOptions, result) {
	var md = result.metaData;
	var rows = result.rows;
	var records = [];
	var nameMap = {};

	if(md && rows) {
		for (var i = 0; i < rows.length; i++) {
			  var row = rows[i];
			  var rec = {};
			  for(var j = 0; j < md.length; j++) {
				  var mdrec = md[j];
				  var val = row[j];
				  
				  // Turn null into undefined so $exist work properly in minimongo
				  if(val === null) {
					  val = undefined;
				  }
				  
				  if(sqlOptions.booleanTrueValue !== undefined) {
					  if(val === sqlOptions.booleanTrueValue) {
						  val = true;
					  } else if(val === sqlOptions.booleanFalseValue) {
						  val = false;
					  }
				  }
				  var name = mdrec['name'];
				  
				  if(name === "ID") {
					  rec["_id"] = val;
					  continue;
				  }
				  
				  var name2 = sqlOptions.sqlConvertColumnNames ? nameMap[name] : name;
				  if(!name2) {
					  var name2 = OracleDB._columnNameO2M(name);
					  nameMap[name] = name2;
				  }
				  rec[name2] = val;
			  }
			  records.push(rec);
		}
		
		result.records = records;
		delete result.rows;
	}

	return result;
}

OracleDB.executeCommand = function(connection, sql, sqlParameters, sqlOptions) {
	  sqlParameters = sqlParameters || {};
	  sqlOptions = sqlOptions || {};
	  
	  var future = new Future;
	  var onComplete = future.resolver();

	  NpmModuleOracledb.getConnection(
			    connection,
			    Meteor.bindEnvironment(
			  
			    function (err, db) {
			        if (err) {
			          throw err;
			        }
			        if(sqlOptions.sqlDebug) {
			        	console.log(sql, sqlParameters);
			        }
			  	  	db.execute(
					      sql,
					      sqlParameters,
					      sqlOptions,
					      Meteor.bindEnvironment(function(err, result) {
					    	  
					    	  db.release(
					    			  function(err) {
					    			      if (err) {
					    			        console.error(err.message);
					    			      }
					    			  });
					    	  
							  onComplete(err, result);
						  },
					      future.resolver()  // onException
					      ));
			      },
			      future.resolver()  // onException
			    )
			  );

	  return OracleDB._processResult(sqlOptions, future.wait());
};

OracleDB.prototype.executeCommand = function(sql, sqlParameters, sqlOptions) {
	var self = this;
	
	return OracleDB.executeCommand(self.connection, sql, sqlParameters, sqlOptions);
};

OracleDB.prototype._refreshTable = function(sqlTable, oracleOptions) {
	  var self = this;
	  var tableResult = self._tables[sqlTable];
	  
	  if(!tableResult) {
		  var tableSql = "select table_name, status, num_rows from user_tables where table_name = upper(:1)";
		  var tableResult = self.executeCommand(tableSql, [sqlTable])
	  
	  	  if(!tableResult.records || tableResult.records.length === 0) {
	  		  // Table does not exist in a database
	  		  return;
	  	  }
	  	  	    		  
	  	  self._tables[sqlTable] = tableResult;
	  }
	  
	  var colSql = "select column_name, column_id, data_type, data_length, data_precision, data_scale, nullable, default_length from user_tab_columns where table_name = upper(:1) order by column_id";
	  var colResult = self.executeCommand(colSql, [sqlTable], oracleOptions)
	  var columns = {};
	  var ccn = {};
	  for(var key in colResult.records) {
		  var rec = colResult.records[key];
		  if(oracleOptions.sqlConvertColumnNames) {
			  columns[rec.columnName] = rec;
			  ccn[OracleDB._columnNameO2M(rec.columnName)] = rec.columnName;
		  } else {
			  columns[rec.COLUMN_NAME] = rec;			  
			  ccn[rec.columnName] = rec.columnName;
		  }
	  }
	  tableResult["columns"] = columns;
	  tableResult["ccn"] = ccn;
	  
	  var detSql = 
		  "select table_name, table_name field_name from user_constraints where constraint_type='R' " +
		  "and r_constraint_name in " +
		  "(select constraint_name from user_constraints where constraint_type in ('P','U') and table_name=upper(:0)) ";

	  var detResult = self.executeCommand(detSql, [sqlTable], oracleOptions)
	  var details = [];
	  for(var key in detResult.records) {
		  var rec = detResult.records[key];
		  if(oracleOptions.sqlConvertColumnNames) {
			  details.push({tableName: rec.tableName, fieldName: OracleDB._columnNameO2M(rec.fieldName)});
		  } else {
			  details.push({tableName: rec.TABLE_NAME, fieldName: rec.FIELD_NAME});			  
		  }
	  }
	  tableResult["details"] = details;

	  for(var di in details) {
		  self._ensureTable(details[di].tableName, oracleOptions, sqlTable);
	  }
};

OracleDB.prototype._ensureTable = function(sqlTable, oracleOptions, parentSqlTable) {
	  var self = this;
	  
	  if(!self._tables[sqlTable]) {
		  self._refreshTable(sqlTable, oracleOptions);
		  
		  if(!self._tables[sqlTable]) {
	  		  // Table doesn't exist, we should create it
	  		  var sql;
	  		  var result;
	  		  
	    	  if(parentSqlTable) {
	    		  sql = "create table "+sqlTable+" (id varchar2(17) not null, index_no number(20, 0) not null)";
	    	  } else {
	    		  sql = "create table "+sqlTable+" (id varchar2(17) not null)";	    		  
	    	  }
	  		  if(oracleOptions.sqlDebug) {
	  			  console.log(sql);
	  		  };
	  		  
	    	  result = self.executeCommand(sql, []);

    		  // Add a primary key
	    	  if(parentSqlTable) {
	    		  sql = "alter table "+sqlTable+" add constraint "+sqlTable+"_pk"+" primary key (id, index_no)";
	    	  } else {
		  		  sql = "alter table "+sqlTable+" add constraint "+sqlTable+"_pk"+" primary key (id)";	    		  
	    	  }
	  		  if(oracleOptions.sqlDebug) {
	  			  console.log(sql);
	  		  };
	  		  result = self.executeCommand(sql, []);		    	  

    		  // Add a foreign key
	    	  if(parentSqlTable) {
	    		  sql = "alter table "+sqlTable+" add constraint "+sqlTable+"_fk"+" foreign key (id) references "+parentSqlTable+"(id) on delete cascade";
	    		  if(oracleOptions.sqlDebug) {
	    			  console.log(sql);
	    		  };
	    		  result = self.executeCommand(sql, []);
	    	  }
	    	  
	    	  // Verify, that the table has been created
			  self._refreshTable(sqlTable, oracleOptions);
			  if(!self._tables[sqlTable]) {
	    		  throw new Error("Table creation failed");
	    	  }
	  	  }
	  	  	    		  
	  	  if(parentSqlTable) {
	  		  self._refreshTable(parentSqlTable, oracleOptions);
	  	  }
	  }
};

OracleDB.prototype._ensureColumns = function(sqlTable, doc, oracleOptions, parentSqlTable) {
	  var self = this;
	  var table = self._tables[sqlTable];
	  if(!table) {
		  self._ensureTable(sqlTable, oracleOptions, parentSqlTable);
		  table = self._tables[sqlTable];
		  
		  if(!table) {
			  throw new Error("Failed to create a new table");
		  }
	  }
	  
	  var refreshCols = false;
	  for(var column in doc) {
		  var value = doc[column];

		  column = OracleDB._columnNameM2O(column);

		  if(!table.columns[column]) {
			  var type = "varchar2(4000)";
			  
			  if(typeof value === 'number') {
				  type = "number";
			  } else if (typeof value === 'boolean') {
				  if(typeof oracleOptions.booleanTrueValue === 'number'
					  && typeof oracleOptions.booleanFalseValue === 'number') {
						  type = "number";
						  if(0 <= oracleOptions.booleanTrueValue && oracleOptions.booleanTrueValue < 10 &&
								  0 <= oracleOptions.booleanFalseValue && oracleOptions.booleanFalseValue < 10 ) {
							  type = "number(1)";
						  }
					  } else if(typeof oracleOptions.booleanTrueValue === 'string'
						  && typeof oracleOptions.booleanFalseValue === 'string') {
							  var maxLen = oracleOptions.booleanTrueValue.length;
							 
							  if(maxLen < oracleOptions.booleanFalseValue.length) {
								  maxLen = oracleOptions.booleanFalseValue.length;
							  }
						  type = "varchar2("+maxLen+")"; // "true" or "false"
					  } else {
						  type = "varchar2(4000)"
					  }
			  } else if(value instanceof Date) {
				  type = "date";
			  } else if(value instanceof Array) {
				  continue;
			  } else if(value != null && typeof value === 'object') {
				  continue;				  
			  }
			  var sql = "alter table "+sqlTable+" add ("+column+" "+type+" null)";
			  var result = self.executeCommand(sql, [], oracleOptions)
			  refreshCols = true;
		  }
	  }
	  
	  if(refreshCols) {
		  self._refreshTable(sqlTable, oracleOptions);
	  }
};

OracleDB.prototype.collection = function(name, oracleOptions, callback) {
	  var self = this;
	  if(typeof oracleOptions == 'function') callback = oracleOptions, oracleOptions = {};
	  oracleOptions = oracleOptions || {};

	  try {
	      var collection = new OracleCollection(this, name, oracleOptions);
	      
	      if(collection.oracleOptions.sqlTable) {
	    	  self._ensureTable(collection.oracleOptions.sqlTable, collection.oracleOptions);
	      }
	      
	      if(callback) callback(null, collection);
	      return collection;
	  } catch(err) {
	      if(callback) return callback(err);
	      throw err;
	  }
};
