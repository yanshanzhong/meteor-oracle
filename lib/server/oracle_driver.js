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
  self.db = new OracleDB();
  self.options = options;
  self.oracleOptions = null;
};

//extend from parent class prototype
OracleConnection.prototype = Object.create(MongoInternals.Connection.prototype); // keeps the proto clean
OracleConnection.prototype.constructor = OracleConnection; // repair the inherited constructor

//Returns the Mongo Collection object; may yield.
OracleConnection.prototype.setOracleOptions = function (oracleOptions) {
  var self = this;

  self.oracleOptions = oracleOptions;
};

//Returns the Mongo Collection object; may yield.
OracleConnection.prototype.rawCollection = function (collectionName) {
  var self = this;

  if (! self.db)
    throw Error("rawCollection called before Connection created?");

  var future = new Future;
  self.db.collection(collectionName, self.oracleOptions, future.resolver());
  return future.wait();
};
