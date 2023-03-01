import re
import io
import os
import sys
import logging
import requests
import argparse
import configparser
import xml.etree.ElementTree as ET
from pymarc import MARCReader
from tools import api_query
from tools import aleph_seq_reader                
from tools import parse_sru_response

NAMESPACES = {'srw': 'http://www.loc.gov/zing/srw/'}

class ISNI_notification_checker():
    """
    Class that checks assigned from ISNI notification and bulk load report files as defined in
    ISNI technical documentation: https://isni.org/page/technical-documentation/
    Input file contains MARC21 records, only Aleph sequential format is currently supported
    """
    def __init__(self):
        parser = argparse.ArgumentParser(description="ISNI AtomPub Conversion program")
        parser.add_argument("-i", "--input_path",
            help="Input path of Aleph sequential file of MARC21 records", required=True)
        parser.add_argument("-if", "--isni_file",
            help="Input path of ISNI report or notification XML file", required=True)
        parser.add_argument("-o", "--output_path", 
            help="Output path for Aleph sequential fields or whole MARC21 records, if format is MARC21")
        parser.add_argument("-u", "--update",
            help="If used, current identifier data is checked from ISNI database")
        parser.add_argument("-f", "--format",
            help="File format", choices=['marc21', 'alephseq'], required=True)
        self.args = parser.parse_args()        
        self.check_file_type(self.args.isni_file)
        file_info = self.check_file_type(self.args.isni_file)
        self.identifiers = {}
        if file_info:
            if file_info['type'] == 'notification':
                self.identifiers = self.get_ids_from_notifications(file_info['root'])
            elif file_info['type'] == 'report':
                self.identifiers = self.get_assigned_isnis(file_info['root'])
        self.isni_ids = {'ISNI': {}, 'PPN': {}}
        self.ppns = {}
        self.local_ids = {}
        for id in self.identifiers:
            for id_type in self.identifiers[id]:
                if self.identifiers[id][id_type]:
                    for isni in self.identifiers[id][id_type]:
                        if not isni in self.isni_ids[id_type]:
                            self.isni_ids[id_type][isni] = set()
                        self.isni_ids[id_type][isni].add(id)
        config = configparser.ConfigParser()
        directory = os.path.realpath(os.path.join(os.path.dirname(__file__)))
        config.read(os.path.join(directory, 'config.ini'))
        section = config['ISNI SRU API']
        if self.args.update:
            self.api_query = api_query.APIQuery(config_section=section,
                                          username=os.environ['ISNI_USER'],
                                          password=os.environ['ISNI_PASSWORD'])
        
    def get_assigned_isnis(self, root):
        """
        reads an ISNI report file and returns a dict of local identifiers as keys and ISNIs as values
        :param bulkload_file: bulk load XML report from ISNI
        """
        contributor_ids = {}
        
        for record in root.findall('record'):
            matchingData = False
            isni = ""
            for match in record.findall('matchingData'):
                matchingData = True
            for status in record.findall('isniStatus'):
                for plain in status.findall('isniPlain'):
                    if isni:
                        logging.error("Multiple ISNIs for same record")
                    else:
                        isni = plain.text
            ids = []
            for lid in record.findall('localIdentification'):
                for id in lid.findall('localIdentifier'):
                    id = re.sub("[\(].*?[\)]", "", id.text)
                    ids.append(id)
                    if id in contributor_ids:
                        if isni not in contributor_ids[id]['ISNI']:
                            contributor_ids[id]['ISNI'].append(isni)
                    else:
                        contributor_ids[id] = {"ISNI": isni, "matchingData": matchingData}

        return contributor_ids

    def get_ids_from_notifications(self, root):
        """
        reads an ISNI notification file and returns a dict of local identifiers as keys and ISNIs as values
        :param root: root of ISNI notification XML file
        """
        identifiers = {}
        for notification in root.findall('notification'):
            local_ids = []
            isni_id = None
            ppn = None
            for local_identifier in notification.findall('localIdentifier'):  
                local_ids.append(re.sub("[\(].*?[\)]", "", local_identifier.text))       
            for ppn in notification.findall('identifier/PPN'):
                ppn = ppn.text
            for isni in notification.findall('identifier/ISNI/isniFormatted'):
                isni_id = isni.text
                isni_id = isni_id.replace('ISNI', '')
                isni_id = isni_id.replace(' ', '')
            for id in local_ids:
                if id not in identifiers:
                    identifiers[id] = {'ISNI': set(), 'PPN': set()}
                if isni_id:
                    if identifiers[id]['ISNI'] and isni_id not in identifiers[id]['ISNI']:
                        logging.error("Multiple ISNIs %s for same record %s"
                        %(identifiers[id], id))
                    identifiers[id]['ISNI'].add(isni_id)
                if ppn:
                    if identifiers[id]['PPN'] and ppn not in identifiers[id]['PPN']:
                        logging.error("Multiple ISNIs %s for same record %s"
                        %(identifiers[id], id))
                    identifiers[id]['PPN'].add(ppn)
        for id in identifiers:
            if identifiers[id]['ISNI'] and identifiers[id]['PPN']:
                logging.error("Record %s has PPN and ISNI identifier"%(id))

        return identifiers
                      
    def get_ids_from_reports(self, root):
        """
        reads ISNI bulk report records from ISNI notifications XML file
        :param root: root of ISNI XML bulk load report file
        """   
        with open(self.args.input_path, 'r', encoding="UTF-8") as f:
            isni_path = 'responseRecord/ISNIAssigned'
            ppn_path = 'responseRecord/ISNINotAssigned'
            identifiers = {}
            metadata_path = '/ISNIMetadata'

            tree = ET.parse(f)
            root = tree.getroot()
            own_identifier = None
            isnis = set()
            ppns = set()
            for records in root.findall('srw:records', NAMESPACES):
                for record in records.findall('srw:record', NAMESPACES):
                    for record_data in record.findall('srw:recordData', NAMESPACES):
                        if len(record_data.findall(isni_path)) > 0:
                            assignment_path = isni_path
                            id_path = assignment_path + "/isniUnformatted"
                            for identifier in record_data.findall(id_path):
                                isnis.add(identifier.text)
                        elif len(record_data.findall(ppn_path)) > 0:
                            assignment_path = ppn_path
                            id_path = assignment_path + "/PPN"
                            for identifier in record_data.findall(id_path):
                                ppns.add(identifier.text)
                        if not assignment_path:
                            logging.error("Identifiers missing in record: %s"%path)
                        else:
                            metadata_path = assignment_path + metadata_path
                            for metadata in record_data.findall(metadata_path):
                                for sources in metadata.findall("sources"):
                                    for code in sources.findall("codeOfSource"):
                                        
                                        if code.text == contributor_id:
                                            for source_id in sources.findall("sourceIdentifier"):
                                                own_identifier = re.sub("[\(].*?[\)]", "", source_id.text)
                                                if own_identifier in identifiers:
                                                    identifiers[own_identifier]['ISNI'].update(isnis)
                                                    identifiers[own_identifier]['PPN'].update(ppns)
                                                else:
                                                    identifiers[own_identifier] = {"ISNI": isnis, "PPN": ppns, "URI": []}
                                for info in metadata.findall("externalInformation"):
                                    for uri in info.findall("URI"):
                                        identifiers[own_identifier]["URI"].append(uri.text)

            if len(isnis) == 0 and len(ppns) == 0:
                logging.error("Identifiers missing in file: %s"%input_path)
            if len(isnis) > 1 or len(ppns) >1:
                logging.error("Multiple identifiers in file: %s"%input_path)
            if isnis and ppns:
                logging.error(input_path)

        for id in identifiers:
            if identifiers[id]["ISNI"] and identifiers[id]["PPN"]:
                logging.error("Record %s has PPN and ISNI identifier"%id)
        return identifiers

    def check_identifiers(self):
        """
        reads ISNI bulk report records from ISNI notifacations XML file
        """ 
        local_ids = set()
        reader = aleph_seq_reader.AlephSeqReader(open(self.args.input_path, 'r', encoding="utf-8"))
        record_id = None
        record = ""
        records = []

        while record is not None:
            try:
                record = next(reader, None)   
            except Exception as e:
                logging.exception(e) 
            if record:
                if record['001']:
                    record_id = record['001'].data
                    local_ids.add(record['001'].data)
                    records.append(record)
                else:
                    continue
            else:
                continue

        current_ids = {}
        isnis = {}
        for id_type in self.isni_ids:
            for id in self.isni_ids[id_type]:
                if len(self.isni_ids[id_type][id]) > 1:
                    for local_id in self.isni_ids[id_type][id]:
                        if local_id not in local_ids:
                            logging.error("Local id %s for %s identifier %s not found"
                            %(local_id, id_type, id))
                    logging.error("Multiple local identifiers %s for %s identifier %s"
                    %(self.isni_ids[id_type][id], id_type, id))
        for id in self.identifiers:
            if id in local_ids:
                notified_isnis = self.identifiers[id]['ISNI']
                if self.args.update:
                    response = self.api_query.get_data_with_local_identifiers("NLFIN", id, "FI-ASTERI-N")
                    current_isni = parse_sru_response.get_isni(response)
                    if current_isni in isnis:
                        logging.warning("Duplicate local identifiers: %s, %s in ISNI %s"
                        %(isnis[current_isni], id, current_isni))
                    else:
                        isnis[current_isni] = [id]
                    if current_isni != notified_isni:
                        logging.warning("ISNI %s for record %s changed"
                        %(current_isni, id))
                else:
                    if notified_isnis:
                        if len(notified_isnis) > 1:
                            logging.error("Local id %s has multiple ISNI identifiers %s"
                            %(id, notified_isnis))
                        elif len(notified_isnis) == 1:
                            next(iter(notified_isnis))
                            notified_isni = next(iter(notified_isnis))
                            isnis[notified_isni] = id
        for isni in isnis:
            current_ids[isnis[isni]] = isni
        self.write_isni_fields(current_ids, records)

    def check_file_type(self, file_path):
        # Note: succesful typo
        notification_tags = ['NOTIFICATIONS'] 
        report_tags = ['succesful', 'unSuccesful', 'noMatch']

        with open(file_path, 'r', encoding="UTF-8") as f:
            tree = ET.parse(f)
            root = tree.getroot()
            xml_type = None
            if root.tag in notification_tags:
                xml_type = 'notification'
            if root.tag in report_tags:
                xml_type = 'report'
            if xml_type:                
                return {'root': root, 'type': xml_type}
            else: 
                logging.error('File %s is not an ISNI XML file'%file_path)
        
    def write_isni_fields(self, isnis, records):
        """
        writes ISNI identifiers into Aleph Sequential format
        :param input_path: original MARC21 records in Aleph Sequential format
        :param output_path: new MARC21 fields 024 that have new ISNI identifiers and other ids in original input
        """
        handle = None
        if self.args.output_path:
            handle = io.open(self.args.output_path, 'w', encoding = 'utf-8', newline='\n')
                        
        if self.args.format == "marc21":
            reader = MARCReader(open(self.args.input_path, 'rb'), to_unicode=True)
        elif self.args.format == "alephseq":                     
            reader = aleph_seq_reader.AlephSeqReader(open(self.args.input_path, 'r', encoding="utf-8"))
        record_id = None

        for record in records:
            record_id = record['001'].data
            if record_id in isnis:
                isni = isnis[record_id]
                fields = []
                isni_found = False
                false_match = False
                for field in record.get_fields("024"):
                    if field['2'] and field['a']:
                        if field['2'] == "isni":
                            if field['a']:
                                if isni == field['a']:
                                    isni_found = True
                                else:
                                    false_match = True
                                    logging.error("ISNI %s changed to %s for record %s"
                                    %(field['a'], isni, record_id))
                    if not false_match:
                        if self.args.format == "alephseq":
                            f = self.create_aleph_seq_field(record_id,
                                                            field.tag,
                                                            field.indicators[0],
                                                            field.indicators[1],
                                                            field.subfields)
                            fields.append(f)
                        if self.args.format == "marc21":
                            field.append(field)                            
                    else:    
                        false_match = False
                if not isni_found:
                    if self.args.format == "marc21":
                        if self.args.output_path:
                            handle.write(record)
                        else:
                            # TODO: not working, as_marc function produces another leader to record
                            sys.stdout.buffer.write(record.as_marc())
                    if self.args.format == "alephseq":
                        fields.append(record_id + " 0247  L $$a" + isni + "$$2isni")
                        for f in fields:
                            if self.args.output_path:
                                handle.write(f + "\n")
                            else:    
                                sys.stdout.write(f + "\n")

        if handle:
            handle.close()
                            
    def create_aleph_seq_field(self, record_id, tag, indicator_1, indicator_2, subfields):
        """
        converts MARC21 field data into Aleph library system's sequential format
        """
        seq_field = record_id
        seq_field += " " + tag + indicator_1 + indicator_2
        seq_field += " L "
        for idx in range(0, len(subfields), 2):
            seq_field += "$$"
            seq_field += subfields[idx] + subfields[idx + 1]
        return seq_field
 
if __name__ == "__main__":
    inm = ISNI_notification_checker()
    inm.check_identifiers()
