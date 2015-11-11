
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

	if(self.oracleOptions.sqlAddId) {
		sql = "SELECT rownum as id, c.* FROM ("+sql+") c";
	}
	
	var originalSelector = selector;
	selector = OracleSelector.process(selector);

	if(selector && selector["."]) {
		var where = selector["."];

		if(where && where !== "") {
			sql = "SELECT * FROM ("+sql+") WHERE "+where;
		}		
	}

	var sorter = undefined;
	var orderBy = "";
	if(options.sort) {
		sorter = OracleSorter.process(options.sort, options);
		orderBy = sorter["."];
	}
	
	if(orderBy && orderBy != "") {
		sql = sql + " ORDER BY " + orderBy;
	}
	
	var cl = OracleFields._prepare(fields);
	
	// Adding fields projection here
	if(cl["."]) {
		var s = OracleFields.getColumnList(self.db._tables[sqlTable], cl["."]);
		sql = "SELECT "+s+" FROM ("+sql+")";
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
	
	var details = [];
	
	if(sqlTable) {
		var tableDesc = self.db._tables[sqlTable];
		
		self.db._ensureSelectableColumns(sqlTable, selector["*"], self.oracleOptions);

		for(var di in tableDesc.details) {
			var detRec = tableDesc.details[di];
			var detTableName = detRec.tableName.toLowerCase();
			
	    	  self.db._ensureTable(detTableName, self.oracleOptions);
	    	  
			// Exclude detail if it's not in the field list
			if(fields && !fields[detRec.fieldName]) {
				continue;
			}
			
			var detSql = "SELECT * FROM "+detTableName+" WHERE id IN (SELECT id FROM ("+sql0+"))";
			var detSqlParameters = [];

			if(selector && selector[detRec.fieldName] && selector[detRec.fieldName]["."]) {
				self.db._ensureSelectableColumns(detTableName, selector[detRec.fieldName]["*"], self.oracleOptions);

				var where = selector[detRec.fieldName]["."];

				if(where && where !== "") {
					detSql = detSql + " AND " + where; 
				}		
			}
			
			if(sorter && sorter[detRec.fieldName] && sorter[detRec.fieldName]["."]) {
				detSql = detSql + " ORDER BY " + sorter[detRec.fieldName]["."] + ", id, index_no";
			} else {
				detSql = detSql + " ORDER BY id, index_no"
			}
			
			// Adding fields projection here
			var dcl = cl[detRec.fieldName];
			
			if(dcl && dcl["."]) {
				var s = OracleFields.getColumnList(self.db._tables[sqlTable], dcl["."], true);

				detSql = "SELECT "+s+" FROM ("+detSql+")";
			}
			
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
	
	var dbCursor = new OracleCollection.Cursor(self, sql, sqlParameters, details, originalSelector, fields, options);
	  
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
		
		if(value instanceof Array) {
			var sqlTable2 = OracleDB._tableNameM2O(column);
			
			sqlTable2 = sqlTable2.toLowerCase();
			
			// Detail table
			for(var index in value) {
				var doc2 = value[index];
				deferred.push([doc2, sqlTable2, options, null, sqlTable, doc["_id"], index]);				
			}
			continue;
		} else if(value instanceof Date) {
			// Fall through
		} else if(value instanceof Object) {
			var sqlTable2 = OracleDB._tableNameM2O(column);
			
			sqlTable2 = sqlTable2.toLowerCase();
			
			deferred.push([value, sqlTable2, options, null, sqlTable, doc["_id"], 0]);				
			continue;
		}
		
		column = OracleDB._columnNameM2O(column);
		
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
	
	self.db._ensureColumns(sqlTable, doc, self.oracleOptions, parentSqlTable, true);
	
	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);
	
	// Process deferred details
	for(var di in deferred) {
		var dvi = deferred[di];
		OracleCollection.prototype._insert.apply(self, dvi);
	}
	
	if(callback) callback(null, result);

	return result;
};

OracleCollection.prototype.remove = function(selector, options, callback) {
	var self = this;

	var sqlTable = self.oracleOptions.sqlTable;
	if(!sqlTable) {
		 throw new Error("Missing sqlTable in oracle options for remove operation");
	}
	var sql = "DELETE FROM " + sqlTable;
	
	var sqlParameters = [];
	
	selector = OracleSelector.process(selector);

	if(selector && selector["."]) {
		var where = selector["."];
		
		if(where && where !== "") {
			sql = sql+" WHERE "+where;
		}		
	}

	self.db._ensureSelectableColumns(sqlTable, selector["*"], self.oracleOptions);
	
	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);
	
	if(callback) {
		callback(null, result);
	}
	
	return result;
};

OracleCollection.prototype.update = function(selector, modifier, options, callback) {
	var self = this;

	var sqlTable = self.oracleOptions.sqlTable;
	if(!sqlTable) {
		 throw new Error("Missing sqlTable in oracle options for remove operation");
	}
	
	modifier = OracleModifier.process(modifier);
	
	var sql = "UPDATE " + sqlTable + " SET ";
	
	if(modifier && modifier["."]) {
		sql = sql + modifier["."];
	}
	
	var sqlParameters = modifier["$"];
	
	selector = OracleSelector.process(selector);

	if(selector && selector["."]) {
		var where = selector["."];

		if(where && where !== "") {
			sql = sql+" WHERE "+where;
		}		
	}

	self.db._ensureColumns(sqlTable, modifier["*"], self.oracleOptions);
	self.db._ensureSelectableColumns(sqlTable, selector["*"], self.oracleOptions);
	
	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);

	result = { "nMatched" : result.rowsAffected, "nUpserted" : 0, "nModified" : result.rowsAffected, result: result };

	if(callback) callback(null, result);
	
	return result;
};

OracleCollection.Cursor = function (collection, sql, sqlParameters, details, selector, fields, options) {
	  var self = this;
	  self.collection = collection;
	  self.sql = sql;
	  self.sqlParameters = sqlParameters;
	  self.details = details;
	  
	  self._selector = selector;
	  self._field = fields;
	  self._options = options;
	  
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

	  var result = undefined;
	  
	  try {
		  result = self.collection.db.executeCommand(self.sql, self.sqlParameters, self.collection.oracleOptions);
	  } catch(ex) {
		console.log("ERROR in FIND: ", {selector: self._selector, fields: self._fields, options: self._options});
		throw ex;  
	  }
	  
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
				  indexNo = objrec.indexNo;
				  delete objrec.indexNo;
				  
				  targetField.push(objrec);
			  }
		  }
	  }
};

OracleCollection.prototype.drop = function(callback) {
	var self = this;

	var sqlTable = self.oracleOptions.sqlTable;
	if(!sqlTable) {
		 throw new Error("Missing sqlTable in oracle options for drop operation");
	}
		
	var tableDesc = self.db._tables[sqlTable];
	
	var detResults = [];
	
	for(var di in tableDesc.details) {
		var detRec = tableDesc.details[di];
		var detTableName = detRec.tableName;
		
		var oracleOptions = EJSON.clone(self.oracleOptions);
		delete oracleOptions.sqlTable;
		
		var detColl = self.db.collection(detTableName, oracleOptions);
		var result = detColl.drop();
		
		detResults.push(result);
	}
	
	delete self.db._tables[sqlTable];
	
	var sql = "DROP TABLE " + sqlTable;
	
	var sqlParameters = [];
	
	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);

	result.detResults = detResults;
	
	if(callback) {
		callback(null, result);
	}
	
	return result;
};

OracleCollection.prototype.ensureIndex = function(fields, options, callback) {
	var self = this;
	options = options || {};

	var sqlTable = self.oracleOptions.sqlTable;
	if(!sqlTable) {
		 throw new Error("Missing sqlTable in oracle options for drop operation");
	}
	
	var results = [];
	var cl = OracleFields._prepare(fields);
	
	// Adding fields projection here
	if(false)
	if(cl["."]) {
		var s = OracleFields.getColumnList(self.db._tables[sqlTable], cl["."], false, true);
		
		if(s) {
		var sql = "CREATE "+(options.bitmap?"BITMAP ":"")+(options.unique?"UNIQUE ":"")+"INDEX "+sqlTable+"_I1 ON "+sqlTable+"("+s+")";
		var sqlParameters = [];
		
		var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);

		results.push(result);
		}
	}

	if(callback) {
		callback(null, results);
	}

};
