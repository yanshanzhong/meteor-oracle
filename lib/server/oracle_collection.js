var path = Npm.require('path');
var Fiber = Npm.require('fibers');
var Future = Npm.require(path.join('fibers', 'future'));

OracleCollection = function (db, collectionName, options) {
	  var self = this;
	  
	  self.db = db;
	  self.collectionName = collectionName;
	  self.options = options;
};

OracleCollection.prototype.find = function(selector, fields, options) {
	var self = this;

	// Construct the SQL
	var sql = self.options.sql || "SELECT * FROM "+self.collectionName;
	var sqlParameters = EJSON.clone(self.options.sqlParameters) || [];
	var sqlScn = self.options.sqlScn;

	var where = null;

	if(selector) {
		var s = new OracleSelector(selector, sqlParameters);
		where = s._docMatcher;
	}

	if(self.options.sqlAddId) {
		sql = "SELECT rownum as id, c.* FROM ("+sql+") c";
	}
	
	if(where && where !== "") {
		sql = "select * from ("+sql+") WHERE "+where;
	}
	
	var orderBy = "";
	if(options.sort) {
		var sorter = new Minimongo.Sorter(options.sort, options);

		var i = 0;

		for(key in sorter._sortSpecParts) {
			var part = sorter._sortSpecParts[key];
			
			if(i > 0) {
				orderBy = orderBy + ", ";
			}
			orderBy = orderBy + part.path;
			if(part.ascending === false) {
				orderBy = orderBy + " DESC";
			} 
			i++;
		}
	}
	
	if(orderBy && orderBy != "") {
		sql = sql + " ORDER BY " + orderBy;
	}
	
	if(options.skip) {
		sql = "SELECT rownum as rowno, c.* FROM ("+sql+") c";
		sql = "SELECT * FROM ("+sql+") WHERE rowno > :skip";
		sqlParameters.push(options.skip);
	}
	
	if(options.limit) {
		sql = "SELECT * FROM ("+sql+") WHERE rownum <= :limit";
		sqlParameters.push(options.limit);
	}
	
	if(sqlScn) {
		sql = "SELECT * FROM ("+sql+") AS OF SCN :scn";
		sqlParameters.push(sqlScn);
	}
	
	// console.log(selector, fields, options, sql);
	
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

	  if(self.collection.options.sqlDebug) {
		  console.log("sql: ", self.sql);
		  console.log("sqlParameters: ", self.sqlParameters);
	  }

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
