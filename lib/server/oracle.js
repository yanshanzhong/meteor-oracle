var oracledb = Npm.require('oracledb');
 
oracledb.getConnection(
  {
    user          : "meteor",
    password      : "meteor",
    connectString : "localhost/XE"
  },
  function(err, connection)
  {
    if (err) {
      console.error(err.message);
      return;
    }
    connection.execute(
      "SELECT * from tab",
      [],
      function(err, result)
      {
        if (err) {
          console.error(err.message);
          return;
        }
        console.log(result.rows);
      });
  });
