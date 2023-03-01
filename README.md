# MARC21ToISNIAtomPub
A tool to transform MARC21 files from MARC21 format into ISNI AtomPub requests. 

#### From MARC21 to ISNI AtomPub XML request

```
usage: 
    python converter.py with command line arguments
    
    mandatory parameters:
    -f format: "marc21", "alephseq", "gramex" (input file in ISO 2709 format, Aleph Sequential file or CSV for Gramex)
    -F config_file_path: File path for configuration file structured for Python ConfigParser (e. g. config.ini file in main directory)
    -a authority_files: file path for MARC21 authority files 
    -r resource_files: file path for MARC21 
    -d output_directory: output directory where converted XML files are written
    -v, validation_file: for development testing enter file path of ISNI Atom Pub Request XSD file to validate XML requests
    -i identifier: requestor's own identifier attached to ISNI requests
    -t identity_types: choice of "persons", "organisations" or "all" to include in request files 
    optional parameters: 
    -c concat: concatenate all request into one file
    -D dirmax: if each request is written into one XML file, files are divided into subdirectories for one request 
                Default number is 100.
    -it, identity_types: Restrict requested records either to persons or organisations, use either persons or organisations
    -M, modified_after: Request records modified or created on or after the set date formatted YYYY-MM-DD
    -C, created_after: Request records created on or after the set date formatted YYYY-MM-DD
    -u, until: Request records created or modified before the set date formatted YYYY-MM-DD
    -l, id_list: Path of text file containing local identifiers, one in every row, of records to be requested to ISNI requestor
    -I, input_raport_list: Path of CSV file containing merge instructions for ISNI requests, formatted like file output_raport_list parsed from ISNI response
    -R, output_raport_list: File name of CSV file raport for unsuccesful ISNI requests
    -O, output_isni_list: File name for Aleph sequential MARC21 fields 024 where received ISNI identifiers are written along existing identifiers
    -m, mode: Use string 'write', to write requests into a directory or 'send' to send them to ISNI production or 'test' to send them to ISNI accept (
               
    Use config.ini for configurations:
    Fill baseurls of APIs as plain text and search parameters JSON formatted e.g. {"recordSchema": "isni-e", "operation": "searchRetrieve"}
```