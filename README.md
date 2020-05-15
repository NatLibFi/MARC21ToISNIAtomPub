# MARC21ToISNIAtomPub
A tool to transform MARC21 files from MARC21 format into ISNI AtomPub requests. The files contain a lot of temporary code to make a library specific batch load for National Library of Finland.

#### From MARC21 to ISNI AtomPub XML request

```
usage: 
    python converter.py with command line arguments
    
    mandatory parameters:
    -f  format: "marc21", "alephseq" (input file in either ISO 2709 format or Aleph Sequential text file)
    -af authority_files: file path for MARC21 authority files 
    -rf resource_files: file path for MARC21 
    -od output_directory: output directory where converted XML files are written
    -id identifier: requestor's own identifier attached to ISNI requests
    -it identity_types: choice of "persons", "organisations" or "all" to include in request files 
    optional parameters:
    -log output file path for logging 
    -max max_number: maximum number of titles of works to be included for one identity
    -c concat: concatenate all request into one file
    -dm dirmax: if each request is written into one XML file, files are divided into subdirectories for one request 
                Default number is 100.
```
