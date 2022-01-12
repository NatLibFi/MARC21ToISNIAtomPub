import logging
from xml.dom.minidom import parseString
import xml.etree.cElementTree as ET
from lxml import etree
import os
import io
import re
import sys
import openpyxl
import argparse
import configparser
import requests
from datetime import datetime
from isni_request import create_xml
from marc21_converter import MARC21Converter
from tools import parse_atompub_response
from tools import parse_sru_response
from tools import xlsx_raport_writer
from tools import api_query

class Converter():
    """
    A class that transform files to ISNI Atom Pub XML format.
    """
    
    def __init__(self):
        parser = argparse.ArgumentParser(description="ISNI AtomPub Conversion program")
        parser.add_argument("-f", "--format",
            help="File format", choices=['marc21', 'alephseq'])
        parser.add_argument("-af", "--authority_files",  
            help="File containing identity data")
        parser.add_argument("-rf", "--resource_files",  
            help="File containing titles of works of authors")
        parser.add_argument("-od", "--output_directory",  
            help="Output directory for ISNI AtomPub XML files")
        parser.add_argument("-vf", "--validation_file",  
            help="Enter file path of ISNI Atom Pub Request XSD file to validate XML requests")
        parser.add_argument("-log", "--log_file",
            help="Output file for error logging")
        parser.add_argument("-id", "--identifier",  
            help="Identifiers of the database of requestor, e. g. FI-ASTERI-N", required=True)
        parser.add_argument("-max", "--max_number", type=int,
            help="Maximum number of titles for one identity")
        parser.add_argument("-c", "--concat",
            help="Concatenate XML request into one file")
        parser.add_argument("-dm", "--dirmax", type=int,
            help="Number of output XML files in one directory, default 100")
        parser.add_argument("-it", "--identity_types",
            help="Restrict requested records either to persons or organisations", choices=['persons', 'organisations'])
        input_group = parser.add_mutually_exclusive_group()
        input_group.add_argument("-ma", "--modified_after",  
            help="Request records modified or created on or after the set date formatted YYYY-MM-DD")
        input_group.add_argument("-ca", "--created_after",  
            help="Request records created on or after the set date formatted YYYY-MM-DD")
        input_group.add_argument("-il", "--id_list",  
            help="Path of text file containing local identifiers, one in every row, of records to be requested to ISNI requestor")
        input_group.add_argument("-irl", "--input_raport_list",  
            help="Path of CSV file containing merge instructions for ISNI requests")
        parser.add_argument("-orl", "--output_raport_list",  
            help="File name of CSV file raport for unsuccesful ISNI requests")
        parser.add_argument("-oil", "--output_marc_fields",
            help="File name for Aleph sequential MARC21 fields code 024 where received ISNI identifiers are written along recent identifiers")
        parser.add_argument("-m", "--mode",
            help="Mode of program: Write requests into a directory or send them to ISNI", choices=['write', 'send'], required=True)
        args = parser.parse_args()
        self.converter = None
        self.convert_to_atompub(args)

    def convert_to_atompub(self, args):
        """
        convert input data to ISNI AtomPub XML format
        :param args: command line arguments parsed by ConfigParser
        """
        logging.getLogger().setLevel(logging.INFO)
        self.modified_after = None
        self.created_after = None
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        section = self.config['ISNI SRU API']                      
        self.sru_api_query = api_query.APIQuery(config_section=section,
                                      username=os.environ['ISNI_USER'],
                                      password=os.environ['ISNI_PASSWORD'])
        if args.mode == "send":
            if args.output_raport_list:
                self.raport_writer = xlsx_raport_writer.RaportWriter(args.output_raport_list)
            else:
                logging.error("Parameter -orl/--output_raport_list missing. Give file path for output raport")
                sys.exit(2)
        if args.modified_after:
            self.modified_after = datetime.date(datetime.strptime(args.modified_after, "%Y-%m-%d"))
        if args.created_after:
            self.created_after = datetime.date(datetime.strptime(args.created_after, "%Y-%m-%d"))
        
        if args.mode == 'write':
            if not args.output_directory:
                logging.error("Mode is set to write. Set output directory parameter for output files")
                sys.exit(2)
            
        if (args.authority_files or args.resource_files) and not args.format:
            logging.error("Using authority_files or resource_files command line arguments but argument format is missing.")
            sys.exit(2)

        loglevel = logging.INFO
        if args.log_file:
            handler = logging.FileHandler(args.log_file, 'w', 'utf-8') # or whatever
            logger = logging.getLogger()
            logger.setLevel(loglevel)
            logger.addHandler(handler)
            logger.propagate = False
        logging.info("Conversion started: %s"%datetime.now().replace(microsecond=0).isoformat())

        requested_ids = set()
        if args.id_list:
            with open(args.id_list, 'r', encoding='utf-8') as fh:
                for row in fh:
                    requested_ids.add(row.rstrip())
        
        merge_instructions = {}
        if args.input_raport_list:
            wb = openpyxl.load_workbook(args.input_raport_list)
            ws = wb.active
            
            for idx, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row)):
                local_id = None
                if row[0].value:
                    local_id = str(row[0].value).strip()
                else:
                    logging.error("Local id missing in row number %s"%idx)
                if local_id:
                    if row[4].value:
                        if row[4].value in ['merge', 'isNot', 'resend']:
                            merge_instructions[local_id] = {'instruction': row[4].value, 'identifiers': []}
                        else:
                            logging.error("Incorrect instruction %s in row number %s"%(row[4].value, row[0].row))
                    if local_id in merge_instructions:
                        for col in ws.iter_cols(min_row=row[0].row, max_row=row[0].row, min_col=6, max_col=ws.max_column):
                            if col[0].value:
                                merge_instructions[local_id]['identifiers'].append(str(col[0].value).strip())
                                requested_ids.add(local_id)

        self.converter = MARC21Converter()
        records = self.converter.get_author_data(args, requested_ids)
        concat = False
        xmlschema = None
        dirmax = 100
        if args.dirmax:
            dirmax = args.dirmax
        if args.validation_file:
            with open(args.validation_file, 'rb') as fh:
                xmlschema_doc = etree.parse(fh)
                xmlschema = etree.XMLSchema(xmlschema_doc)   
        if args.output_directory and not os.path.exists(args.output_directory):
            os.mkdir(args.output_directory)
        dirindex = 0
        xml = ""
        if args.concat:
            concat_path = os.path.join(args.output_directory, "concat.xml")
            with open(concat_path, 'ab+') as concat_file:
                concat_file.write(bytes("<?xml version=\"1.0\" ?>\n<root>\n", "UTF-8"))
        idx = 0
        logging.info("Starting to convert records...")

        isnis = {}
        for record_id in records:
            merge_instruction = None
            merge_identifiers = None
            if merge_instructions:
                if record_id not in merge_instructions:
                    continue
                else:
                    merge_instruction = merge_instructions[record_id]['instruction']
                    merge_identifiers = merge_instructions[record_id]['identifiers']
            elif self.created_after:
                creation_date = datetime.date(datetime.strptime(records[record_id]['creation date'], "%Y-%m-%d"))
                if creation_date < self.created_after:
                    continue
            elif self.modified_after:
                modification_date = datetime.date(datetime.strptime(records[record_id]['modification date'], "%Y-%m-%d"))
                if modification_date < self.modified_after:
                    continue
            xml = create_xml(records[record_id], merge_instruction, merge_identifiers)
            if not xml:
                continue
            if idx % dirmax == 0:
                dirindex += 1
            if args.output_directory:
                subdir = os.path.join(args.output_directory, str(dirindex).zfill(3))
                if not (os.path.exists(subdir)) and not concat:
                    os.mkdir(subdir)
            if xmlschema:
                if not self.valid_xml(record_id, xml, xmlschema):
                    continue
            if args.mode == "send":
                logging.info("Sending record %s"%record_id)
                response = self.send_xml(xml)
                if 'possible matches' in response:
                    possible_matches = []
                    for pm in response['possible matches']:
                        if 'id' in pm:
                            result = self.sru_api_query.search_with_id('ppn', pm['id'])
                            isni_id = parse_sru_response.get_isni_identifier(result)
                            if isni_id and len(isni_id) == 16:
                                pm['id'] = isni_id
                        else:
                            logging.error('Record %s has missing possible match id'%record_id)
                    if len(possible_matches) == 1:
                        if possible_matches[0] == records[record_id]['ISNI']:
                            xml = create_xml(records[record_id], records[record_id]['ISNI'])
                            if xmlschema:
                                if not self.valid_xml(record_id, xml, xmlschema):
                                    sys.exit(2)
                            response = self.send_xml(xml) 
                            if 'possible matches' in response:
                                response['error'] = 'Resubmit for record failed'

                if 'isni' in response:
                    isnis[record_id] = response['isni']
                self.raport_writer.handle_response(response, record_id, records[record_id])
            elif args.mode == "write":
                if args.concat:
                    xml_path = concat_path
                else:    
                    xml_path = os.path.join(subdir, record_id + ".xml")
                self.write_xml(xml, xml_path, args.concat)
            idx += 1
        logging.info("Conversion done for %s items"%idx)
        if args.concat:
            with open(args.output_directory+"/concat.xml", 'ab+') as concat_file:
                concat_file.write(bytes("</root>", "UTF-8"))
        if isnis:
            self.converter.write_isni_fields(isnis, args)

    def valid_xml(self, record_id, xml, xmlschema):
        try:
            string_xml = io.StringIO(xml)
            doc = etree.parse(string_xml)
            xmlschema.assert_(doc)
            return True
        except AssertionError as e:
            logging.error("Record %s XML AssertionError : %s"%(record_id, e))

    def write_xml(self, xml, file_path, concat):
        """
        Write XML records to file. Creates directory dirname, and creates XML Request files into the folder.
        :param dirname: path of the directory, where XML records are written.
        :param dirmax: dirmax default is 100, so it makes 100 request xml files per folder before creating a new one.
        :param dirindex: index number of directory
        """ 
        xml = xml.replace("<?xml version=\"1.0\" ?>", "")       
        if concat:
            xmlfile = open(file_path, 'ab+')
            xmlfile.write(bytes(xml, 'UTF-8'))
            xmlfile.flush()
        else:
            xmlfile = open(file_path, 'wb+')
            xmlfile.write(bytes(xml, 'UTF-8'))
            xmlfile.close()

    def send_xml(self, xml):
        headers = {'Content-Type': 'application/atom+xml; charset=utf-8'}
        section = self.config['ISNI ATOMPUB API']
        response = requests.post(section.get('baseurl'), data=xml.encode('utf-8'), headers=headers)
        xml = response.text
        parsed_response = parse_atompub_response.get_response_data_from_response_text(xml)
        return parsed_response

if __name__ == '__main__':
    Converter()