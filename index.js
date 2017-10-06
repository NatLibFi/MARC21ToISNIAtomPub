/* This script is for converting Aleph sequentials to Marc21 binary */

'use strict';

var fs = require('fs');
var Serializers = require('marc-record-serializers');
var Record = require('marc-record-js');
var path = require('path');

var filePath = path.resolve(__dirname, 'test/');
var asterimetadata = path.resolve(filePath, "asteri_full.seq");

var reader = new Serializers.AlephSequential.Reader(fs.createReadStream(asterimetadata));

var convertedRecords = [];

reader.on('data', function(record){
    convertedRecords.push(Serializers.ISO2709.toISO2709(record))

});

reader.on('end', function(){
   var file = fs.createWriteStream(path.resolve(filePath, "asteri_full.mrc"));
   file.on('error', function (err) {
       console.log(err)
   });
   convertedRecords.forEach(function(c){
       file.write(c);
   });
   file.end();
});
