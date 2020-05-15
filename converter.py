import logging
from xml.dom.minidom import parseString
import xml.etree.cElementTree as ET
from lxml import etree
import os
import io
import re
import sys
import argparse
import pickle
from datetime import datetime
from isni_request import create_xml
from marc21_data_collector import MARC21DataCollector
from resource_list import ResourceList

class Converter:
    """
    A class to transform files to ISNI Atom Pub XML format.
    :param authorities_path: input file of authority data
    :param input_format: format of the input data (e. g. MARC21)
    :param resource_path: file path for bioliographical records
    """
    
    def __init__(self, args):
        logging.getLogger().setLevel(logging.INFO)
        """
        max_number_of_titles = 10 #maximum number of titles to include for one identity
        authorities_path = args.authority_files #file path for authority records
        resource_files
        self.input_format = input_format #format if the authority file, "MARC21" only current valid format
        """

        loglevel = logging.INFO
        if args.log_file:
            handler = logging.FileHandler(args.log_file, 'w', 'utf-8') # or whatever
            logger = logging.getLogger()
            logger.setLevel(loglevel)
            logger.addHandler(handler)
            logger.propagate = False
        logging.info("Conversion started: %s"%datetime.now().replace(microsecond=0).isoformat())

        if args.format.lower() in ['marc21', 'alephseq']:
            data_collector = MARC21DataCollector()
            resources = ResourceList(args.resource_files, args.format).titles          
            self.records = data_collector.get_author_data(args, resources)
            concat = False
            dirmax = 100
            validation_file = None
            if args.concat:
                concat = args.concat
            if args.dirmax:
                dirmax = args.dirmax
            if validation_file:
                validation_file = args.validation_file
            self.write_xml(args.output_directory, dirmax, concat, validation_file)
        else:
            logging.error("invalid input format")
            sys.exit(2)

    def write_xml(self, dirname, dirmax, concat, validation_file):
        """
        Write XML records to file. Creates directory dirname, and creates XML Request files into the folder.
        :param dirname: path of the directory, where XML records are written.
        :param dirmax: dirmax default is 100, so it makes 100 request xml files per folder before creating a new one.
        :param concat: records are concatenad into one if True.
        :param validation_file: file path for AtomPub request XSD, all requests are validated if the path is given here.
        """
        logging.info("Starting to write ISNI requests...")
        
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        i = 1
        dirinc = 0
        xml = ""
        if concat:
            concat_path = os.path.join(dirname, "concat_request.xml")
            with open(concat_path, 'ab+') as concat_file:
                concat_file.write(bytes("<?xml version=\"1.0\" ?>\n<root>\n", "UTF-8"))
        xmlschema = None
        if validation_file:
            with open('ISNI_atompub_scheme.xsd', 'rb') as fh:
                xmlschema_doc = etree.parse(fh)
                xmlschema = etree.XMLSchema(xmlschema_doc)
        for record_id in self.records:
            xml = create_xml(record_id, self.records[record_id])
            test_xml = io.StringIO(xml)
            doc = etree.parse(test_xml)
            if i % dirmax == 0:
                dirinc += 1
            subdir = os.path.join(dirname, str(dirinc).zfill(3))
            if not (os.path.exists(subdir)) and not concat:
                os.mkdir(subdir)
            if concat:
                xml = xml.replace("<?xml version=\"1.0\" ?>", "")
                xmlfile = open(concat_path, 'ab+')
                xmlfile.write(bytes(xml, 'UTF-8'))
                xmlfile.flush()
                i += 1
            else:
                xml_path = os.path.join(subdir, "request_" + str(i).zfill(5) + ".xml")
                xmlfile = open(xml_path, 'wb+')
                xmlfile.write(bytes(xml, 'UTF-8'))
                xmlfile.close()
                i += 1
            if validation_file:
                try:
                    xmlschema.assert_(doc)
                except AssertionError as e:
                    logging.error("XML AssertionError : %s"%e)
                    logging.error("%s"%xml)
                    
        if concat:
            with open(dirname+"/concat_request.xml", 'ab+') as concat_file:
                concat_file.write(bytes("</root>", "UTF-8"))
            
        logging.info("Conversion done for {} items".format(i))

def main():
    parser = argparse.ArgumentParser(description="ISNI AtomPub Conversion program")
    parser.add_argument("-f", "--format",
        help="File format", choices=['marc21', 'alephseq'], required=True)
    parser.add_argument("-af", "--authority_files",  
        help="File containing identity data", required=True)
    parser.add_argument("-rf", "--resource_files",  
        help="File containing titles of works of authors", required=True)
    parser.add_argument("-od", "--output_directory",  
        help="Output directory for ISNI AtomPub XML files", required=True)
    parser.add_argument("-vf", "--validation_file",  
        help="Validate AtomPub request with XSD file")
    parser.add_argument("-log", "--log_file",
        help="Output file for error logging")
    parser.add_argument("-id", "--identifier",  
        help="Identifiers of the database of requestor, e. g. FI-ASTERI-N", required=True)
    parser.add_argument("-max", "--max_number", type=int,
        help="Maximum number of titles for one identity")
    parser.add_argument("-c", "--concat",
        help="Concatenate XML request into one file")
    parser.add_argument("-dm", "--dirmax", type=int,
        help="Number of output XML files in one directory")
    parser.add_argument("-it", "--identity_types",
        help="Maximum number of titles for one identity", choices=['persons', 'organisations', 'all'], required=True)
    args = parser.parse_args()
    Converter(args)

if __name__ == '__main__':
    main()