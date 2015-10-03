Package.describe({
  name: 'amisystem:meteor-oracle',
  version: '0.0.1',
  // Brief, one-line summary of the package.
  summary: 'Oracle Database Driver for Meteor',
  // URL to the Git repository containing the source code for this package.
  git: 'https://github.com/amisystem/meteor-oracle.git',
  // By default, Meteor will default to using README.md for documentation.
  // To avoid submitting documentation, set this field to null.
  documentation: 'README.md'
});

Npm.depends({
	'oracledb': '1.2.0'
});

Package.onUse(function(api) {
  api.export(['NpmModuleOracledb', 'NpmModuleOracledbVersion'], 'server');
  api.export(['OracleInternals', 'OracleDB', 'OracleTest', 'OracleSelector'], 'server');
  api.export(['Oracle'], ['client', 'server']);
  api.versionsFrom('1.1.0.3');
  api.use('underscore');
  api.use('callback-hook', 'server');
  api.use('mongo', ['client', 'server']);
  api.use('minimongo', 'server');
  api.use('ejson', 'server');
  api.addFiles('lib/server/wrapper.js', 'server');
  api.addFiles('lib/server/oracle_driver.js', 'server');
  api.addFiles('lib/server/remote_collection_driver.js', 'server');
  api.addFiles('lib/server/collection.js', ['client', 'server']);
  api.addFiles('lib/server/oracle_collection.js', 'server');
  api.addFiles('lib/server/oracle_db.js', 'server');
  api.addFiles('lib/server/oracle_selector.js', 'server');
  api.addFiles('lib/server/helpers.js', 'server');
});

Package.onTest(function(api) {
  api.use('tinytest');
  api.use('amisystem:meteor-oracle');
  api.addFiles('lib/server/tests/oracle_tests.js', 'server');
  api.addFiles('lib/server/tests/oracle_db_tests.js', 'server');
  api.addFiles('lib/server/tests/collection_tests.js', 'server');
  api.addFiles('lib/server/tests/oracle_selector_tests.js', 'server');
});
