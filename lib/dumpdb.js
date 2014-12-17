var sqlTableDump = require('./src/sqldump.data.js')
, config = require("./msSqlDump_config.js").config
, fs = require("fs")
, sql = require("node-sqlserver-unofficial")
, exec = require('child_process').exec 
, database = config.database
, connectionString = config.connectionString
, outDir = __dirname + '/../../'
, deleteFolderRecursive = function(path) {
    var files = [];
    if( fs.existsSync(path) ) {
        files = fs.readdirSync(path);
        files.forEach(function(file,index){
            var curPath = path + "/" + file;
            if(fs.lstatSync(curPath).isDirectory()) { // recurse
                deleteFolderRecursive(curPath);
            } else { // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
};
;


if(process.argv.length >= 3) {
  database = process.argv[2];
  connectionString = connectionString.replace("Database={" + config.database + "}", "Database={" + database + "}");
}

console.log('Dumping Database: ' + database + ' ...');


//clean up
deleteFolderRecursive(outDir+database);
fs.mkdirSync(outDir+database);
fs.mkdirSync(outDir+database+'/Data');

exec('"'+__dirname +'/bin/ScriptDb.exe" -server:'+config.server+' -db:'+database+' -outDir:"'+outDir+'" -ScriptAsCreate ', function (err, stdout, stderr) {
    if(err){ 	
      console.log(err);				
    }else {
      console.log(stdout);	
    }
});


var query = " SELECT '['+Table_Schema+'].['+ Table_Name+']' as TableName FROM INFORMATION_SCHEMA.TABLES " 
  + " where TABLE_TYPE = 'BASE TABLE' "
  + " AND Table_Name not like 'C!_%' ESCAPE '!' "
  + " AND Table_Name not like 'sys%' "
  + " order by Table_Schema, Table_Name ";

sql.open( connectionString, function( err, conn ) {
  conn.query( query, function(err, results){
    var i, options;
    if(err)  console.log(err);
    for(i=0; i<results.length; i++) {
      options = {
        table: results[i].TableName
        , where: false
        , columns: '*'
        , connectionString: connectionString
      }
      sqlTableDump(options, function(err, data) {
        if(err) {
          console.log(err);
          return;
        }
        fs.writeFile(outDir+database+'/Data/'+data.table.replace(/\[|\]/g,'')+'.sql', data.sqlStr, function(err) {
              if(err) {
                  console.log('ERROR: ' + err);
              } else {
                  //console.log('The file '+data.table.replace(/\[|\]/g,'')+'.Data.sql was saved!');
              }
          });
          return;
      });
    }
  });
});

/*
SELECT '"'+Table_Schema+'.'+ Table_Name+'",' FROM INFORMATION_SCHEMA.TABLES
where TABLE_TYPE = 'BASE TABLE'
AND Table_Name not like 'C\_%' ESCAPE '\'
order by Table_Schema, Table_Name
*/