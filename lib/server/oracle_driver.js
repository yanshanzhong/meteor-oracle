var path = Npm.require('path');
var OracleDB = NpmModuleOracledb;
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

OracleLocalCollection = function (name) {
	  var self = this;

	  LocalCollection.call(this, name);
};

//extend from parent class prototype
OracleLocalCollection.prototype = Object.create(LocalCollection.prototype); // keeps the proto clean
OracleLocalCollection.prototype.constructor = OracleLocalCollection; // repair the inherited constructor

OracleLocalCollection.prototype.find0 = OracleLocalCollection.prototype.find;
OracleLocalCollection.prototype.find = function(collectionName, selector, options) {
	  var mongoOptions = options;
	  
	  var dbCursor = this.find0(collectionName, selector, options);

	  dbCursor.nextObject = function(callback) {
			 var self = this;
			 
			 if(self.rows === undefined) {
				 self.rows = self.fetch();
				 if(mongoOptions.sort) {
					 self.rows.sort(function (a, b) {
						 for(col in mongoOptions.sort) {
							 if(a[col] > b[col]) {
								 return mongoOptions.sort[col];
							 } else if((a[col] < b[col])) {
								 return -mongoOptions.sort[col];
							 }
						 }
						 
						 return 0;
					 });
				 }
				 self.rowIndex = mongoOptions.skip || 0;
			 }
			 
			 var r = null;
			 
			 if(self.rowIndex < self.rows.length && self.rowIndex < (mongoOptions.skip || 0) + (mongoOptions.limit || self.rows.length)) {
				 r = self.rows[self.rowIndex++];
		  	 }
			 
			 callback(null, r);
		  };
	  
	  return dbCursor;
};

OracleConnection = function (options) {
  var self = this;

  var mongoUrl = process.env.MONGO_URL;
  MongoInternals.Connection.call(this, mongoUrl, options);
  
  // Closing the mongo connection created by parent
  self.close();
  self.db = null;
  self.options = options;
};

//extend from parent class prototype
OracleConnection.prototype = Object.create(MongoInternals.Connection.prototype); // keeps the proto clean
OracleConnection.prototype.constructor = OracleConnection; // repair the inherited constructor

OracleConnection.prototype.rawCollection = function (collectionName) {
	  var self = this;

	  //if (! self.db)
	  //  throw Error("rawCollection called before Connection created?");

	  var future = new Future;
	  var onComplete = future.resolver();
	  var sql = "SELECT * from "+collectionName;
	  var sqlParameters = [];

//	  console.log(sql);
	  sql = self.sql || sql;
	  sqlParameters = self.sqlParameters || sqlParameters;

	  OracleDB.getConnection(
			    self.options,
			    Meteor.bindEnvironment(
			  
			      function (err, db) {
			        if (err) {
			          throw err;
			        }
			        
			  	  db.execute(
					      sql,
					      sqlParameters,
					      Meteor.bindEnvironment(function(err, result) {
					    	  var coll = new OracleLocalCollection();
					    	  var md = result.metaData;
					    	  var rows = result.rows;
					    	  var i;
					    	  for (i = 0; i < rows.length; i++) {
					    		  var row = rows[i];
					    		  var j;
					    		  var rec = {};
					    		  for(j = 0; j < md.length; j++) {
					    			  var mdrec = md[j];
					    			  rec[mdrec['name']] = row[j];
					    		  }
					    		  coll.insert(rec);
					    	  }

					    	  db.release(
					    			    function(err) {
					    			      if (err) {
					    			        console.error(err.message);
					    			      }
					    			    });
							  onComplete(err, coll);
						  },
					      future.resolver()  // onException
					      ));
			      },
			      future.resolver()  // onException
			    )
			  );

	  return future.wait();
	};

OracleConnection.prototype._observeChanges = function (
		    cursorDescription, ordered, callbacks) {
		  var self = this;

		  var cursor = self.find(cursorDescription.collectionName, cursorDescription.selector, cursorDescription.options);

		  cursor.forEach(function (doc, index, selfForIteration) {
			  var fields = EJSON.clone(doc);
			  delete fields._id;
			  callbacks.added(doc._id, fields);
		  }, null);
		};
