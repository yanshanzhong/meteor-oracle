# meteor-oracle

This package allows thousands of enterprises to build and deploy Meteor applications that access their data stored in Oracle databases.

```javascript
var coll = new Oracle.Collection("todos");

coll.insert();
var rows = coll.find({}, {}).fetch();
```

### Package Installation

Follow the installation steps listed for node.js package "oracledb".

* Install the small, free Oracle Instant Client libraries if your database is remote. Or use a locally installed database such as the free Oracle XE release.
* Run npm install oracledb to install from the NPM registry.
* Set environment variable LD_LIBRARY_PATH accordingly, e.g.:
** export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:~/workspace/instantclient_11_2
* See https://github.com/oracle/node-oracledb for more information about node.js package "node-oracledb"
* See https://github.com/oracle/node-oracledb/blob/master/INSTALL.md for detailed installation steps


Add meteor-oracle package to your meteor application.

    meteor add metstrike:meteor-oracle

### License

Released under the MIT license. See the LICENSE file for more info.
