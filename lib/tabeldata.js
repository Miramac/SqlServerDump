var sql = require("node-sqlserver-unofficial")
;


module.exports = sqlTableDump;

function sqlTableDump (options, cb) {
  cb = (typeof cb === "function") ? cb : null;
  var err = null, query, columnstr;
  if(!options.connectionString) {
    err = new Error("This script cannot run without a connection string");
    if(cb) {
        cb(err);
        return;
    } else {
        throw err;
    }
  }
  
  options.columns = (options.columns) ? (options.columns instanceof Array) ? options.columns.join(",") : options.columns : "*";
  
  query = "SELECT "+options.columns+" FROM "+options.table+" WHERE 1=1";
  query += (options.where) ? " AND ( "+options.where+" ) " : "";

  sql.open( options.connectionString, function( err, conn ) {
    conn.queryRaw( query, function(err, results){
      if(err)  cb(query, err);
      if(results.rows.length === 0) {
        err = "No data received from "+options.table;
        cb(err, query);
    return;
       }
      var i, exportColumns = [],  tableDumpStr = "INSERT INTO "+options.table+" ( ";
      for(i=0; i<results.meta.length; i++) {
        if(results.meta[i].sqlType.indexOf("identity") === -1) {
          tableDumpStr += ((i>0) ? ",":"") +results.meta[i].name;
          exportColumns.push({
            index: i, 
            type: results.meta[i].type
          });
        }
      }
      tableDumpStr += ")\n";
      
      //data rows
      var rowStr, colIndex, cellVal;
      for(i=0;i<results.rows.length;i++) {
        rowStr = ((i>0) ? "UNION ALL\n":"") + "SELECT ";
        for(colIndex=0; colIndex<exportColumns.length; colIndex++) {
          cellVal = results.rows[i][exportColumns[colIndex].index];
          cellVal = (typeof cellVal === 'string') ? "'"+cellVal.replace(/'/g,"''")+"'" 
                  : (cellVal instanceof Date)  ? jsDateToSqlDate(cellVal) : cellVal;
                  
          rowStr += ((colIndex>0) ? ",":"") + cellVal;
        }
        tableDumpStr += rowStr+"\n";
      }
      
      if(cb) {
        cb(err, {table: options.table, sqlStr: tableDumpStr});
      }
      return {table: options.table, sqlStr: tableDumpStr};
    });
  });
}
function jsDateToSqlDate(date) {
  if(date instanceof Date) {
    return '\'' + date.getUTCFullYear() + '-' +
      ('00' + (date.getUTCMonth() + 1)).slice(-2) + '-' +
      ('00' + date.getUTCDate()).slice(-2) + ' ' +
      ('00' + date.getUTCHours()).slice(-2) + ':' +
      ('00' + date.getUTCMinutes()).slice(-2) + ':' +
      ('00' + date.getUTCSeconds()).slice(-2) + '\'';
  }
  return null;
}
  