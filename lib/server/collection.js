// options.connection, if given, is a LivedataClient or LivedataServer
// XXX presently there is no way to destroy/clean up a Collection

/**
 * @summary Namespace for OracleDB-related items
 * @namespace
 */
Oracle = {};

Oracle.Collection = function (name, options) {
	options = options || {};

	if (!options._driver) {
		  if (Meteor.isServer) {
		      options._driver = new OracleInternals.RemoteCollectionDriver(options);
		  }
	}

	Mongo.Collection.call(this, name, options);	
};

//extend from parent class prototype
Oracle.Collection.prototype = Object.create(Mongo.Collection.prototype); // keeps the proto clean
Oracle.Collection.prototype.constructor = Oracle.Collection; // repair the inherited constructor

if(Meteor.isServer) {

	Oracle.Collection.prototype.setOracleOptions = function(oracleOptions) {
		var self = this;
	
		self._driver.oracle.setOracleOptions(oracleOptions);
	};

	Oracle.setDefaultOracleOptions = function (oracleOptions) {
		OracleConnection.setDefaultOracleOptions(oracleOptions);
	};
}
