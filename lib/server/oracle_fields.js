
OracleFields = {};

OracleFields._prepare = function(fields) {
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

OracleFields.getColumnList = function(tableDesc, fields, isDetailTable, excludeId) {
	var cl = "";
	
	if(tableDesc) {
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
					
					ffn = OracleDB.q(ffn);
					
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
				var s = OracleDB.q("_id");
				
				if(isDetailTable) {
					s = OracleDB.q("_id")+", "+OracleDB.q("_indexNo");
				}
				
				if(cl.length > 0) {
					s = s + ", ";
				}
				
				cl = s + cl;
			}
		} else {
			for(var cc in tableDesc.ccn) {
				var c = tableDesc.ccn[cc];
				
				c = OracleDB.q(c);
				
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
				
				ffn = OracleDB.q(ffn);
								
				if(cl.length > 0) {
					cl = cl + ", ";
				}
				
				cl = cl + ffn;
				
			} else {
				throw new Error("OracleCollection.find() when collection is query based then fields parameter can't be exclusions");
			}
		}
	}
	
	return cl;
};

