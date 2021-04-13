# MARC21ToISNIAtomPub
A tool to transform MARC21 files from MARC21 format into ISNI AtomPub requests. 

#### From MARC21 to ISNI AtomPub XML request

```
usage: 
    python converter.py with command line arguments
    
    mandatory parameters:
    -f  format: "marc21", "alephseq" (input file in either ISO 2709 format or Aleph Sequential text file)
    -af authority_files: file path for MARC21 authority files 
    -rf resource_files: file path for MARC21 
    -od output_directory: output directory where converted XML files are written
    -vf, validation_file: for development testing enter file path of ISNI Atom Pub Request XSD file to validate XML requests
    -id identifier: requestor's own identifier attached to ISNI requests
    -it identity_types: choice of "persons", "organisations" or "all" to include in request files 
    optional parameters:
    -log output file path for logging 
    -max max_number: maximum number of titles of works to be included for one identity
    -c concat: concatenate all request into one file
    -dm dirmax: if each request is written into one XML file, files are divided into subdirectories for one request 
                Default number is 100.
    -it, identity_types: Restrict requested records either to persons or organisations, use either persons or organisations
    -ma, modified_after: Request records modified or created on or after the set date formatted YYYY-MM-DD
    -ca, created_after: Request records created on or after the set date formatted YYYY-MM-DD
    -il, id_list: Path of text file containing local identifiers, one in every row, of records to be requested to ISNI requestor
    -irl, input_raport_list: Path of CSV file containing merge instructions for ISNI requests, formatted like file output_raport_list parsed from ISNI response
    -orl, output_raport_list: File name of CSV file raport for unsuccesful ISNI requests
    -oil, output_isni_list: File name for Aleph sequential MARC21 fields 024 where received ISNI identifiers are written along existing identifiers
    -m, mode: Use string 'write' or 'send' to write requests into a directory or send them to ISNI
               
    Use config.ini for configurations:
    Fill baseurls of APIs as plain text and search parameters JSON formatted e.g. {"recordSchema": "isni-e", "operation": "searchRetrieve"}
```
