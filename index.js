/* This script is for converting Aleph sequentials to Marc21 */

'use strict';

var fs = require('fs');
var Serializers = require('marc-record-serializers');
var path = require('path');

if(process.argv.length < 4){
    console.log("Give input and output files.")
    process.exit(1);
}

var filePath = path.resolve(__dirname, 'test/');
var asterimetadata = path.resolve(process.argv[2]);

var reader = new Serializers.AlephSequential.Reader(fs.createReadStream(asterimetadata));

var file = fs.createWriteStream(path.resolve(process.argv[3]));
file.on('error', function (err) {
   console.log(err)
});

reader.on('data', function(record){
    file.write(Serializers.ISO2709.toISO2709(record))
});

reader.on('end', function(){
   file.end();
});