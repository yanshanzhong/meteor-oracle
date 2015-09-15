var path = Npm.require('path');
var Fiber = Npm.require('fibers');
var Future = Npm.require(path.join('fibers', 'future'));

OracleCollection = function (db, collectionName, options) {
	  var self = this;
	  
	  self.db = db;
	  self.collectionName = collectionName;
	  self.options = options;
};

OracleCollection.prototype._keysel2where = function(key, selector) {
	var self = this;
	var where = "";

	if(selector) {
		var i = 0;

		for(opkey in selector) {
			var value = selector[opkey];
			
			var op = " = ";

			if(i > 0) {
				where = where + " AND ";
			} else {
				where = where + "(";
			}
			
			if(typeof value === "object") {
				where = where + self._keysel2where(key, value);				
			} else if(opkey === "$ne") {
				op = " <> ";
				if(value === "null") {
					where = where + key + " IS NOT NULL";					
				} else {
					where = where + key + " IS NULL OR " + key + op + "'" + value + "'";
				}
			} else if(opkey === "$eq") {
				op = " = ";
				if(value === "null") {
					where = where + key + " IS NULL";					
				} else {
					where = where + key + op + "'" + value + "'";
				}
			} else if(opkey === "$exists") {
				where = where + key + " IS NOT NULL";					
			} else {
				if(opkey === "$gt") {
					op = " > ";
				} else if(opkey === "$gte") {				
					op = " >= ";
				} else if(opkey === "$lt") {				
					op = " < ";
				} else if(opkey === "$lte") {				
					op = " <= ";
				}
				where = where + key + op + "'" + value + "'";
			}			
			i++;
		}
		
		if(i > 0) {
			where = where + ")";
		}
	}
	
	return where;
};

OracleCollection.prototype._sel2where = function(selector) {
	
	var self = this;
	var where = "";

	if(selector) {
		var i = 0;

		for(var key in selector) {
			var value = selector[key];
			var op = " = ";

			if(i > 0) {
				where = where + " AND ";
			} else {
				where = where + "(";
			}
			
			if(typeof value === "object") {
				where = where + self._keysel2where(key, value);				
			} else {
				where = where + key + op + "'" + value + "'";
			}
			
			i++;
		}
		
		if(i > 0) {
			where = where + ")";
		}
	}
	
	return where;
};

OracleCollection.prototype.find = function(selector, fields, options) {
	var self = this;

	var where = self._sel2where(selector);

	// Construct the SQL
	var sql = self.options.sql || "SELECT * FROM "+self.collectionName;
	
	sql = "SELECT rownum as id, c.* FROM ("+sql+") c";
	
	if(where && where != "") {
		sql = "select * from ("+sql+") WHERE "+where;
	}
	
	var orderBy = "";
	if(options.sort) {
		var i = 0;

		for(key in options.sort) {
			var value = options.sort[key];
			
			if(i > 0) {
				orderBy = orderBy + ", ";
			}
			orderBy = orderBy + key;
			if(value && value < 0) {
				orderBy = orderBy + " DESC";
			} 
			i++;
		}
	}
	
	if(orderBy && orderBy != "") {
		sql = sql + " ORDER BY " + orderBy;
	}
	
	var sqlParameters = self.options.sqlParameters || [];
	
	var dbCursor = new OracleCollection.Cursor(self, sql, sqlParameters);
	  
	return dbCursor;
};

OracleCollection.Cursor = function (collection, sql, sqlParameters) {
	  var self = this;
	  self.collection = collection;
	  self.sql = sql;
	  self.sqlParameters = sqlParameters;
	  
	  self.objects = [];
	  self.nextObjectIndex = 0;
	  self.loaded = false;
};

OracleCollection.Cursor.prototype.nextObject = function (callback) {
	  var self = this;
	  var err = null;
	  var r = null;
	  
	  if(!self.loaded) {
		  err = self._loadObjects();
		  self.loaded = true;
	  }
	  
	  if(self.nextObjectIndex < self.objects.length) {
		  r = self.objects[self.nextObjectIndex++];
	  }
	  
	  // console.log(err, self.nextObjectIndex, self.objects.length, r);
	  
	  callback(err, r);
};

OracleCollection.Cursor.prototype.count = function (callback) {
	  var self = this;
	  var err = null;
	  var cnt = null;
	  
	  if(!self.loaded) {
		  err = self._loadObjects();
		  self.loaded = true;
	  }
	  
	  cnt = self.objects.length;
	  
	  callback(err, cnt);
};

OracleCollection.Cursor.prototype.rewind = function () {
	  var self = this;
	  
	  self.loaded = false;
	  self.objects = [];
	  self.nextObjectIndex = 0;
};

OracleCollection.Cursor.prototype._loadObjects = function () {
	  var self = this;

	  var future = new Future;
	  var onComplete = future.resolver();

	  // console.log(self.sql);

	  NpmModuleOracledb.getConnection(
			    self.collection.options,
			    Meteor.bindEnvironment(
			  
			      function (err, db) {
			        if (err) {
			          throw err;
			        }
			        
			  	  db.execute(
					      self.sql,
					      self.sqlParameters,
					      Meteor.bindEnvironment(function(err, result) {
					    	  if(!err) {
					    	  var md = result.metaData;
					    	  var rows = result.rows;
					    	  var i;

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
					    		  self.objects.push(rec);
					    	  }
					    	  }
					    	  
					    	  db.release(
					    			    function(err) {
					    			      if (err) {
					    			        console.error(err.message);
					    			      }
					    			    });
							  onComplete(err, null);
						  },
					      future.resolver()  // onException
					      ));
			      },
			      future.resolver()  // onException
			    )
			  );
	  
	  return future.wait();
};
