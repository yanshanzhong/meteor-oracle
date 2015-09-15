

OracleDB = function () {
	  var self = this;
	  
	  self.db = NpmModuleOracledb;
	  self.db.maxRows = 1000;
};
/*
OracleDB.prototype.open = function(callback) {
	callback(null, {});
};

OracleDB.prototype.close = function(force, callback) {
	callback(null, {});
};
*/

OracleDB.prototype.collection = function(name, options, callback) {
	  var self = this;
	  if(typeof options == 'function') callback = options, options = {};
	  options = options || {};

	  try {
	      var collection = new OracleCollection(this, name, options);
	      if(callback) callback(null, collection);
	      return collection;
	    } catch(err) {
	      if(callback) return callback(err);
	      throw err;
	    }
};

