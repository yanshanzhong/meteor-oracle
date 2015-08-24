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
	'oracledb': '1.0.0'
});

Package.onUse(function(api) {
  api.versionsFrom('1.1.0.3');
  api.addFiles('lib/server/oracle.js', 'server');
  api.addFiles('lib/server/methods.js', 'server');
});

Package.onTest(function(api) {
  api.use('tinytest');
  api.use('amisystem:oracle');
  api.addFiles('lib/server/oracle-tests.js', 'server');
});
