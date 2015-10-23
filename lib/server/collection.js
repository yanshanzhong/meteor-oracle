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

	Oracle.setDefaultOracleOptions({
			connection: {
				user: "meteor", 
				password: "meteor", 
				connectString: "localhost/XE"
			},
			sql: null,
			sqlParameters: [],
			sqlScn: null,
			sqlAddId: false,
			sqlDebug: false,
			sqlConvertColumnNames: true,
			booleanTrueValue: "true",
			booleanFalseValue: "false"		
	});
}

Oracle.LocalDate = function (dateStr) {
    var utcDate = new Date(dateStr);
    	
    var localDate = new Date(utcDate.getTime()+utcDate.getTimezoneOffset()*60*1000);

    return localDate;   
}
