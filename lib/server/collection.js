// options.connection, if given, is a LivedataClient or LivedataServer
// XXX presently there is no way to destroy/clean up a Collection

/**
 * @summary Namespace for OracleDB-related items
 * @namespace
 */
Oracle = {};

/**
 * @summary Constructor for a Collection
 * @locus Anywhere
 * @instancename collection
 * @class
 * @param {String} name The name of the collection.  If null, creates an unmanaged (unsynchronized) local collection.
 * @param {Object} [options]
 * @param {Object} options.connection The server connection that will manage this collection. Uses the default connection if not specified.  Pass the return value of calling [`DDP.connect`](#ddp_connect) to specify a different server. Pass `null` to specify no connection. Unmanaged (`name` is null) collections cannot specify a connection.
 * @param {String} options.idGeneration The method of generating the `_id` fields of new documents in this collection.  Possible values:
 * @param {Function} options.transform An optional transformation function. Documents will be passed through this function before being returned from `fetch` or `findOne`, and before being passed to callbacks of `observe`, `map`, `forEach`, `allow`, and `deny`. Transforms are *not* applied for the callbacks of `observeChanges` or to cursors returned from publish functions.
 */

Oracle.Collection = function (name, options) {
	options = options || {};

	if (Meteor.isClient) {
		options.password = null;
	}
	
	if (!options._driver) {
		  if (Meteor.isServer) {
		      options._driver = new OracleInternals.RemoteCollectionDriver(options);
		      options._driver.oracle.sql = options.sql;
		      options._driver.oracle.sqlParameters = options.sqlParameters;		      
		  }
	}

	Mongo.Collection.call(this, name, options);	
};

//extend from parent class prototype
Oracle.Collection.prototype = Object.create(Mongo.Collection.prototype); // keeps the proto clean
Oracle.Collection.prototype.constructor = Oracle.Collection; // repair the inherited constructor
