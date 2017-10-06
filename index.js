/* This script is for converting Aleph sequentials to Marc21 binary */

'use strict';

var fs = require('fs');
var Serializers = require('marc-record-serializers');
var Record = require('marc-record-js');
var path = require('path');

var filePath = path.resolve(__dirname, 'test/');
var asterimetadata = path.resolve(filePath, "asteri_full.seq");

var reader = new Serializers.AlephSequential.Reader(fs.createReadStream(asterimetadata));

var parseRecords = [];
reader.on('data', function(record){
    parseRecords.push(record);
});

reader.on('end', function(){
   console.log(parseRecords[0])
});
