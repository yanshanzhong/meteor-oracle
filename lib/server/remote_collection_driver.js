OracleInternals.RemoteCollectionDriver = function (options) {
  var self = this;
  self.oracle = new OracleConnection(options);
};

_.extend(OracleInternals.RemoteCollectionDriver.prototype, {
  open: function (name) {
    var self = this;
    var ret = {};
    _.each(
      ['find', 'findOne', 'insert', 'update', 'upsert',
       'remove', '_ensureIndex', '_dropIndex', '_createCappedCollection',
       'dropCollection', 'rawCollection'],
      function (m) {
        ret[m] = _.bind(self.oracle[m], self.oracle, name);
      });
    return ret;
  }
});
