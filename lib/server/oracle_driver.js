var path = Npm.require('path');
var Fiber = Npm.require('fibers');
var Future = Npm.require(path.join('fibers', 'future'));

OracleInternals = {};
OracleTest = {};

OracleInternals.NpmModules = {
  Oracledb: {
    version: NpmModuleOracledbVersion,
    module: OracleDB
  }
};

OracleInternals.NpmModule = OracleDB;

// Inherits from MongoConnection
OracleConnection = function (options) {
  var self = this;

  options = options || {};
  options._disableOplog = true;
  
  var mongoUrl = process.env.MONGO_URL;
  MongoInternals.Connection.call(this, mongoUrl, options);
  
  // Closing the mongo connection created by parent
  self.close();
  
  self.options = options;
  self.setOracleOptions({});
};

OracleConnection._defaultOracleOptions = null;

OracleConnection.setDefaultOracleOptions = function(oracleOptions) {
	OracleConnection._defaultOracleOptions = oracleOptions;
	
	// Clear some items which do not make sense
	OracleConnection._defaultOracleOptions.sql = null;
	OracleConnection._defaultOracleOptions.sqlParameters = [];
}

OracleConnection._mergeOptions = function(oracleOptions, defaultOracleOptions) {
	var o = {};
	
	for(var a in defaultOracleOptions) {
		o[a] = defaultOracleOptions[a];
	}
	
	for(var a in oracleOptions) {
		o[a] = oracleOptions[a];
	}
	
	return o;
}

//extend from parent class prototype
OracleConnection.prototype = Object.create(MongoInternals.Connection.prototype); // keeps the proto clean
OracleConnection.prototype.constructor = OracleConnection; // repair the inherited constructor

//Returns the Mongo Collection object; may yield.
OracleConnection.prototype.setOracleOptions = function (oracleOptions) {
  var self = this;

  self.oracleOptions = OracleConnection._mergeOptions(oracleOptions, OracleConnection._defaultOracleOptions);
  self.db = OracleDB.getDatabase(self.oracleOptions.connection);
};

//Returns the Mongo Collection object; may yield.
OracleConnection.prototype.rawCollection = function (collectionName) {
  var self = this;

  if (! self.db)
    throw Error("rawCollection called before Connection created?");

  var future = new Future;

  self.db.collection(collectionName, self.options, self.oracleOptions, future.resolver());

  return future.wait();  
};
