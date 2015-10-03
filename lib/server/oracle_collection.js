
OracleCollection = function (db, collectionName, oracleOptions) {
	  var self = this;
	  
	  self.db = db;
	  self.collectionName = collectionName;
	  self.oracleOptions = oracleOptions;
	  if(self.oracleOptions.sqlTable === undefined) {
		  self.oracleOptions.sqlTable = self.collectionName;
	  }
	  self.oracleOptions.sql = self.oracleOptions.sql || "SELECT * FROM "+self.oracleOptions.sqlTable;
};

OracleCollection.prototype.find = function(selector, fields, options) {
	var self = this;

	// Construct the SQL
	var sql = self.oracleOptions.sql;
	var sqlParameters = EJSON.clone(self.oracleOptions.sqlParameters) || [];

	var where = null;

	if(selector) {
		var s = new OracleSelector(selector, sqlParameters);
		where = s._docMatcher;
	}

	if(self.oracleOptions.sqlAddId) {
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
	
	if(self.oracleOptions.sqlScn) {
		sql = "SELECT * FROM ("+sql+") AS OF SCN :scn";
		sqlParameters.push(self.oracleOptions.sqlScn);
	}
	
	// console.log(selector, fields, options, sql);
	
	var dbCursor = new OracleCollection.Cursor(self, sql, sqlParameters);
	  
	return dbCursor;
};

OracleCollection.prototype.insert = function(doc, options, callback) {
	var self = this;
	var colList = "";
	var valList = "";
	
	var sqlParameters = [];
	var i = 0;
	for(var column in doc) {
		var value = doc[column];
		column = column.toUpperCase();
		if(column === "_ID") {
			column = "ID";
		}
		if(i > 0) {
			colList = colList + ", ";
			valList = valList + ", ";
		}
		colList = colList + column;
		valList = valList + ":" + i;
		sqlParameters.push(value);
		i++;
	}

	var sql = "insert into "+self.oracleOptions.sqlTable+" ("+colList+") values ("+valList+")" 
	if(self.oracleOptions.sqlDebug) {
		console.log("sql: ", sql);
		console.log("sqlParameters: ", sqlParameters);
	}
	
	self.db._ensureColumns(self.oracleOptions.sqlTable, doc, self.oracleOptions);
	
	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);
	
	if(callback) callback(null, result);

	return result;
};

OracleCollection.Cursor = function (collection, sql, sqlParameters) {
	  var self = this;
	  self.collection = collection;
	  self.sql = sql;
	  self.sqlParameters = sqlParameters;
	  
	  self.objects = null;
	  self.nextObjectIndex = 0;
	  self.loaded = false;
};

OracleCollection.Cursor.prototype.nextObject = function (callback) {
	  var self = this;
	  var err = null;
	  var r = null;
	  
	  if(!self.loaded) {
		  self._loadObjects();
		  self.loaded = true;
	  }
	  
	  if(self.nextObjectIndex < self.objects.length) {
		  r = self.objects[self.nextObjectIndex++];
	  }
	  
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

	  if(self.collection.oracleOptions.sqlDebug) {
		  console.log("sql: ", self.sql);
		  console.log("sqlParameters: ", self.sqlParameters);
	  }
	  
	  var result = self.collection.db.executeCommand(self.sql, self.sqlParameters);
	  
	  self.objects = result.records;
};
