
OracleCollection = function (db, collectionName, oracleOptions) {
	  var self = this;
	  
	  self.db = db;
	  self.collectionName = collectionName;
	  self.oracleOptions = oracleOptions;
	  if(self.oracleOptions.sqlTable === undefined) {
		  self.oracleOptions.sqlTable = self.collectionName;
	  }
	  self.oracleOptions.sql = self.oracleOptions.sql;
};

OracleCollection.prototype.find = function(selector, fields, options) {
	var self = this;

	// Construct the SQL
	var sql = self.oracleOptions.sql;
	var sqlTable = null;
	
	if(!sql) {
		 sqlTable = self.oracleOptions.sqlTable;
		 if(!sqlTable) {
			 throw new Error("Missing sqlTable in oracle options");
		 }
		 sql = "SELECT * FROM " + sqlTable;
	}
	
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
		sql = "SELECT * FROM ("+sql+") WHERE "+where;
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
	
	var sql0 = sql;
	
	if(self.oracleOptions.sqlScn) {
		sql = "SELECT * FROM ("+sql+") AS OF SCN :scn";
		sqlParameters.push(self.oracleOptions.sqlScn);
	}
	
	// console.log(selector, fields, options, sql);
	var details = [];
	
	if(sqlTable) {
		var tableDesc = self.db._tables[sqlTable];
		for(var di in tableDesc.details) {
			var detRec = tableDesc.details[di];
			var detTableName = detRec.tableName;
			var detSql = "SELECT * FROM "+detTableName+" WHERE id IN (SELECT id FROM ("+sql0+"))";
			var detSqlParameters = [];
			for(var i in sqlParameters) {
				detSqlParameters.push(sqlParameters[i]);
			}
			if(self.oracleOptions.sqlScn) {
				detSql = "SELECT * FROM ("+sql+") AS OF SCN :scn";
				detSqlParameters.push(self.oracleOptions.sqlScn);
			}
			details.push({detailRecord: detRec, sql:detSql, sqlParameters:detSqlParameters});
		}
	};
	
	var dbCursor = new OracleCollection.Cursor(self, sql, sqlParameters, details);
	  
	return dbCursor;
};

OracleCollection.prototype.insert = function(doc, options, callback) {
	var self = this;
	
	return self._insert(doc, self.oracleOptions.sqlTable, options, callback, null, null, null);
};

OracleCollection.prototype._insert = function(doc, sqlTable, options, callback, parentSqlTable, parent_id, index_no) {
	var self = this;
	var cmds = {};
		
	var colList = "";
	var valList = "";
	
	var sqlParameters = [];
	var i = 0;
	var deferred = [];
	
	if(parentSqlTable) {
		colList = "ID, INDEX_NO";
		valList = ":0, :1";
		sqlParameters.push(parent_id);
		sqlParameters.push(index_no);
		i = 2;
	}
	
	for(var column in doc) {
		var value = doc[column];
		column = column.toUpperCase();
		
		if(value instanceof Array) {
			// Add a prefix to the detail table
			var sqlTable2 = column;
			
			// Detail table
			for(var index in value) {
				var doc2 = value[index];
				deferred.push([doc2, sqlTable2, options, null, sqlTable, doc["_id"], index]);				
			}
			continue;
		}
		
		if(column === "_ID") {
			column = "ID";
		}
		if(i > 0) {
			colList = colList + ", ";
			valList = valList + ", ";
		}
		colList = colList + column;
		valList = valList + ":" + i;
		if(typeof value === 'boolean') {
			if(value === true) {
				value = self.oracleOptions.booleanTrueValue;				
			} else if(value === false) {
				value = self.oracleOptions.booleanFalseValue;
			}
		}
		sqlParameters.push(value);
		i++;
	}

	var sql = "insert into "+sqlTable+" ("+colList+") values ("+valList+")" 
	
	self.db._ensureColumns(sqlTable, doc, self.oracleOptions, parentSqlTable);
	
	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);
	
	// Process deferred details
	for(var di in deferred) {
		var dvi = deferred[di];
		OracleCollection.prototype._insert.apply(self, dvi);
	}
	
	if(callback) callback(null, result);

	return result;
};

OracleCollection.Cursor = function (collection, sql, sqlParameters, details) {
	  var self = this;
	  self.collection = collection;
	  self.sql = sql;
	  self.sqlParameters = sqlParameters;
	  self.details = details;
	  
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

	  var result = self.collection.db.executeCommand(self.sql, self.sqlParameters, self.collection.oracleOptions);
	  
	  self.objects = result.records;
	  
	  if(self.details && self.details.length > 0) {
		  // Make an inverted map of records
		  var recMap = {};
		  for(var i in self.objects) {
			  var rec = self.objects[i];
			  
			  recMap[rec._id] = rec;
		  }
		  
		  for(var i in self.details) {
			  var detail = self.details[i];
			  var detailCursor = new OracleCollection.Cursor(self.collection, detail.sql, detail.sqlParameters, null);
			  detailCursor._loadObjects();
			  for(var obji in detailCursor.objects) {
				  var objrec = detailCursor.objects[obji];

				  var targetRec = recMap[objrec._id];
				  if(!targetRec) {
					  throw new Error("Inconsistent detail records");
					  continue;
				  }
				  var detField = detail.detailRecord.fieldName;
				  var targetField = targetRec[detField];
				  if(!targetField) {
					  targetField = [];
					  targetRec[detField] = targetField;
				  }
				  var indexNo;
				  
				  delete objrec._id;
				  if(self.collection.oracleOptions.sqlConvertColumnNames) {
					  indexNo = objrec.indexNo;
					  delete objrec.indexNo;
				  } else {
					  indexNo = objrec.INDEX_NO;
					  delete objrec.INDEX_NO;					  
				  }
				  
				  targetField[indexNo] = objrec;
			  }
		  }
	  }
};
