
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

OracleCollection.prototype._getColumnList = function(sqlTable, fields, isDetailTable, excludeId) {
	var self = this;
	
	var cl = "";
	
	if(sqlTable) {
		var tableDesc = self.db._tables[sqlTable];

		var incl = undefined;
		
		for(var fn in fields) {
			if(incl === undefined) {
				incl = fields[fn];
			}
			
			if(incl) {
				if(!fields[fn]) {
					throw new Error("Inconsistent fields parameter (mixing includes and excludes)");
				}

				// Formatted field name
				var ffn = OracleDB._columnNameM2O(fn);
				
				if(tableDesc.columns[ffn]) {
					if(cl.length > 0) {
						cl = cl + ", ";
					}
					
					cl = cl + ffn;
				}
			} else {
				if(fields[fn]) {
					throw new Error("Inconsistent fields parameter (mixing includes and excludes)");
				}
			}
		}
		
		if(incl) {
			// Add ID field if it wasn't included explicitly
			if(!excludeId && !fields["_id"]) {
				var s = "ID";
				
				if(isDetailTable) {
					s = s + ", INDEX_NO"
				}
				
				if(cl.length > 0) {
					s = s + ", ";
				}
				
				cl = s + cl;
			}
		} else {
			for(var cc in tableDesc.ccn) {
				var c = tableDesc.ccn[cc];
				
				if(cc === "id") {
					cc = "_id";
				}
				
				if(fields[cc] === undefined) {
					if(cl.length > 0) {
						cl = cl + ", ";
					}
					
					cl = cl + c;					
				}
			}
		}
	} else {
		for(var fn in fields) {
			if(fields[fn]) {

				// Formatted field name
				var ffn = OracleDB._columnNameM2O(fn);
				
				if(cl.length > 0) {
					cl = cl + ", ";
				}
				
				cl = cl + ffn;
				
			} else {
				throw new Error("OracleCollection.find() when collection is query based then fields parameter can't exclusions");
			}
		}
	}
	
	return cl;
};

OracleCollection.prototype._prepareColumnLists = function(fields) {
	var self = this;
	
	var cls = {};
	
	if(!fields) {
		return cls;
	}
	
	for(var fn in fields) {
		var fa = fn.split(".");
		
		var cl = cls;
		
		for(var i = 0; i < fa.length-1; i++) {
			var fi = fa[i];
			if(cl[fi] === undefined) {
				var n = {};
				cl[fi] = n;
				cl = n;
			}
		}
		
		if(cl["."] === undefined) {
			var n = {};
			cl["."] = n;
			cl = n;
		} else {
			cl = cl["."];			
		}
		
		// last field name
		var lfn = fa[fa.length-1];
		
		cl[lfn] = fields[fn];
	}

	return cls;
};

OracleCollection.prototype._prepareSelector = function(selector) {
	var self = this;

	var sls = {};
	
	if(!selector) {
		return sls;
	}
	
	for(var fn in selector) {
		if(typeof fn === 'string' && fn[0] === '.') {
			var fa = fn.slice(1).split(".");

			var sl = sls;
			
			for(var i = 0; i < fa.length; i++) {
				var fi = fa[i];
				
				if(sl[fi] === undefined) {
					var n = {};
					sl[fi] = n;
					sl = n;
				}
			}
			
			if(sl["."] === undefined) {
				sl["."] = selector[fn];
			} else {
				throw new Error("Duplicate embedded selector "+fn+", use $elemMatch to concatenate");
			}
		} else {
			// Add field to sls["."]
			var sl = sls["."];
			
			if(sl === undefined) {
				sl = {};
				sls["."] = sl;
			}
			
			sl[fn] = selector[fn];
		}		
	}

	return sls;
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
	
	selector = self._prepareSelector(selector);

	if(selector && selector["."]) {
		var s = new OracleSelector(selector["."], sqlParameters);
		var where = s._docMatcher;
		if(where && where !== "") {
			sql = "SELECT * FROM ("+sql+") WHERE "+where;
		}		
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
			var column = OracleDB._columnNameM2O(part.path);
			orderBy = orderBy + column;
			if(part.ascending === false) {
				orderBy = orderBy + " DESC";
			} 
			i++;
		}
	}
	
	if(orderBy && orderBy != "") {
		sql = sql + " ORDER BY " + orderBy;
	}
	
	var cl = self._prepareColumnLists(fields);
	
	// Adding fields projection here
	if(cl["."]) {
		var s = self._getColumnList(sqlTable, cl["."]);
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
		
		for(var di in tableDesc.details) {
			var detRec = tableDesc.details[di];
			var detTableName = detRec.tableName;
			
			// Exclude detail if it's not in the field list
			if(fields && !fields[detRec.fieldName]) {
				continue;
			}
			
			var detSql = "SELECT * FROM "+detTableName+" WHERE id IN (SELECT id FROM ("+sql0+"))";
			var detSqlParameters = [];

			if(selector && selector[detRec.fieldName] && selector[detRec.fieldName]["."]) {
				var s = new OracleSelector(selector[detRec.fieldName]["."], sqlParameters);
				var where = s._docMatcher;
				if(where && where !== "") {
					detSql = detSql + " AND " + where; 
				}		
			}
			
			detSql = detSql + " ORDER BY id, index_no"
			// Adding fields projection here
			var dcl = cl[detRec.fieldName];
			
			if(dcl && dcl["."]) {
				var s = self._getColumnList(detRec.tableName, dcl["."], true);

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
		
		if(value instanceof Array) {
			var sqlTable2 = OracleDB._tableNameM2O(column);
			
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

OracleCollection.prototype.remove = function(selector, options, callback) {
	var self = this;

	var sqlTable = self.oracleOptions.sqlTable;
	if(!sqlTable) {
		 throw new Error("Missing sqlTable in oracle options for remove operation");
	}
	var sql = "DELETE FROM " + sqlTable;
	
	var sqlParameters = [];
	
	selector = self._prepareSelector(selector);

	if(selector && selector["."]) {
		var s = new OracleSelector(selector["."], sqlParameters);
		var where = s._docMatcher;
		if(where && where !== "") {
			sql = sql+" WHERE "+where;
		}		
	}

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
	
	var sqlParameters = [];
	
	selector = self._prepareSelector(selector);

	if(selector && selector["."]) {
		var s = new OracleSelector(selector["."], sqlParameters);
		var where = s._docMatcher;
		if(where && where !== "") {
			sql = sql+" WHERE "+where;
		}		
	}

	var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);

	result = { "nMatched" : result.rowsAffected, "nUpserted" : 0, "nModified" : result.rowsAffected, result: result };
	
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

	var sqlTable = self.oracleOptions.sqlTable;
	if(!sqlTable) {
		 throw new Error("Missing sqlTable in oracle options for drop operation");
	}
	
	var results = [];
	var cl = self._prepareColumnLists(fields);
	
	// Adding fields projection here
	if(cl["."]) {
		var s = self._getColumnList(sqlTable, cl["."], false, true);
		var sql = "CREATE "+(options.bitmap?"BITMAP ":"")+(options.unique?"UNIQUE ":"")+"INDEX "+sqlTable+"_I1 ON "+sqlTable+"("+s+")";
		var sqlParameters = [];
		
		var result = self.db.executeCommand(sql, sqlParameters, self.oracleOptions);

		results.push(result);		
	}

	if(callback) {
		callback(null, results);
	}

};
