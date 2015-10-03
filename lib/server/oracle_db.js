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
}

OracleDB._processResult = function(result) {
	var md = result.metaData;
	var rows = result.rows;
	var records = [];
	var i;

	if(md && rows) {
		for (i = 0; i < rows.length; i++) {
			  var row = rows[i];
			  var j;
			  var rec = {};
			  for(j = 0; j < md.length; j++) {
				  var mdrec = md[j];
				  rec[mdrec['name']] = row[j];
			  }
			  if(rec["ID"]) {
				  rec["_id"] = rec["ID"];
				  delete rec["ID"];
			  }
			  records.push(rec);
		}
		
		result.records = records;
		delete result.rows;
	}
	
	return result;
}

OracleDB.executeCommand = function(connection, sql, sqlParameters, sqlOptions) {
	  var self = this;

	  var future = new Future;
	  var onComplete = future.resolver();

	  NpmModuleOracledb.getConnection(
			    connection,
			    Meteor.bindEnvironment(
			  
			    function (err, db) {
			        if (err) {
			          throw err;
			        }
			        console.log(sql, sqlParameters);
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

	  return OracleDB._processResult(future.wait());
};

/*
OracleDB.prototype.open = function(callback) {
	callback(null, {});
};

OracleDB.prototype.close = function(force, callback) {
	callback(null, {});
};
*/

OracleDB.prototype.executeCommand = function(sql, sqlParameters, sqlOptions) {
	var self = this;
	
	return OracleDB.executeCommand(self.connection, sql, sqlParameters || [], sqlOptions || {});
};

OracleDB.prototype._refreshTableColumns = function(sqlTable) {
	  var self = this;
	  var tableResult = self._tables[sqlTable];
	  
	  if(!tableResult) {
		  throw new Error("Table " + sqlTable + " does not exist");
	  }
	  var colSql = "select column_name, column_id, data_type, data_length, data_precision, data_scale, nullable, default_length from user_tab_columns where table_name = upper(:1) order by column_id";
	  var colResult = self.executeCommand(colSql, [sqlTable])
	  var columns = {};
	  for(var key in colResult.records) {
		  var rec = colResult.records[key];
		  columns[rec.COLUMN_NAME] = rec;
	  }
	  tableResult["columns"] = columns;
	  console.log(tableResult);
};

OracleDB.prototype._ensureTable = function(sqlTable, oracleOptions) {
	  var self = this;
	  
	  if(!self._tables[sqlTable]) {
		  var tableSql = "select table_name, status, num_rows from user_tables where table_name = upper(:1)";
		  var tableResult = self.executeCommand(tableSql, [sqlTable])
	  
	  	  if(!tableResult.records || tableResult.records.length === 0) {
	  		  // Table doesn't exist, we should create it
	  		  var sql;
	  		  var result;
	  		  
	  		  sql = "create table "+sqlTable+" (id varchar2(17) not null)";
	  		  if(oracleOptions.sqlDebug) {
	  			  console.log(sql);
	  		  };
		    	  result = self.executeCommand(sql, []);

		    	  // Verify, that the table has been created
		    	  tableResult = self.executeCommand(tableSql, [sqlTable])
		    	  if(!tableResult.records || tableResult.records.length === 0) {
		    		  throw new Error("Table creation failed");
		    	  }
		    	  
		    	  // Add a primary key
	  		  sql = "alter table "+sqlTable+" add constraint "+sqlTable+"_pk"+" primary key (id)";
	  		  if(oracleOptions.sqlDebug) {
	  			  console.log(sql);
	  		  };
		    	  result = self.executeCommand(sql, []);		    	  
	  	  }
	  	  	    		  
	  	  self._tables[sqlTable] = tableResult;
	  	  self._refreshTableColumns(sqlTable);
	  }
};

OracleDB.prototype._ensureColumns = function(sqlTable, doc, oracleOptions) {
	  var self = this;
	  var table = self._tables[sqlTable];
	  if(!table) {
		  self._ensureTable(sqlTable);
		  table = self._tables[sqlTable];
		  
		  if(!table) {
			  throw new Error("Failed to create a new table");
		  }
	  }
	  
	  var refreshCols = false;
	  for(var column in doc) {
		  var value = doc[column];
		  column = column.toUpperCase();
		  if(column === "_ID") {
			  column = "ID";
		  }
		  if(!table.columns[column]) {
			  var type = "varchar2(4000)";
			  if(typeof value === 'number') {
				  type = "number";
			  } else if (typeof value === 'boolean') {
				  type = "varchar2(5)"; // "true" or "false"
			  }
			  var sql = "alter table "+sqlTable+" add ("+column+" "+type+" null)";
			  var result = self.executeCommand(sql, [], oracleOptions)
			  refreshCols = true;
		  }
	  }
	  
	  if(refreshCols) {
		  self._refreshTableColumns(sqlTable);
	  }
};

OracleDB.prototype.collection = function(name, options, oracleOptions, callback) {
	  var self = this;
	  if(typeof options == 'function') callback = options, options = {};
	  options = options || {};

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
