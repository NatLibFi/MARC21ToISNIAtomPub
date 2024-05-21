import re
import os
import logging
import argparse
import configparser
import xml.etree.ElementTree as ET
from pymarc import Field, Subfield
from marc21_converter import MARC21Converter

NAMESPACES = {'srw': 'http://www.loc.gov/zing/srw/'}

def get_identifiers(file_path):
    """
    reads an ISNI notification file and returns a dict of local identifiers as keys and ISNIs as values
    :param file_path: file_path of ISNI notification XML file
    """
    identifiers = {}
    if not os.path.exists(file_path):
        logging.error('File %s not found'%file_path)
    else:
        with open(file_path, 'r', encoding="UTF-8") as f:
            tree = ET.parse(f)
            root = tree.getroot()
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
                        identifiers[id] = {'ISNI': '', 'PPN': ''}
                    if isni_id:
                        if identifiers[id]['ISNI'] and isni_id not in identifiers[id]['ISNI']:
                            logging.error("Multiple ISNIs %s for same record %s"
                            %(identifiers[id], id))
                        identifiers[id]['ISNI'] = isni_id
                    if ppn:
                        if identifiers[id]['PPN'] and ppn not in identifiers[id]['PPN']:
                            logging.error("Multiple ISNIs %s for same record %s"
                            %(identifiers[id], id))
                        identifiers[id]['PPN'] = ppn

    return identifiers

def get_linked_isnis(marc_records, marc_id, isni, linked_ids):
    tags = ['500', '510']
    marc_record = marc_records[marc_id]
    for tag in tags:
        for field in marc_record.get_fields(tag):
            linked_id = None
            if '0' in field and field['0']:
                linked_id = re.sub("[\(].*?[\)]", "", field['0'])
                if linked_id not in linked_ids:
                    linked_ids.add(linked_id)
                    #linked_cluster.add(linked_id)
                    if '001' in marc_record:
                        get_linked_isnis(marc_records, linked_id, isni, linked_ids)

def output_aleph_sequential_fields(identifiers, args):
    """
    writes ISNI identifiers into Aleph Sequential format
    :param identifiers: dict of identifiers, with local identifier as key and ISNI as value
    :param args: command line arguments
    """
    config = None
    # mock args for MARC21Converter class
    args.modified_after = None
    args.created_after = None
    args.until = None
    args.format = 'alephseq'

    if args.config_file_path:
        config = configparser.ConfigParser()
        config.read(args.config_file_path)
    mc = MARC21Converter(config)
    mc.request_ids = set(identifiers.keys())
    marc_records = mc.read_marc_records(args)

    isni_ids = {}

    for marc_id in identifiers:
        isnis = {}
        isni = identifiers[marc_id]['ISNI']
        if isni:
            if isni not in isni_ids:
                isni_ids[isni] = set()
            isni_ids[isni].add(marc_id)
        deletable_field = None
        if marc_id in marc_records and isni:
            linked_ids = {marc_id}
            fields = []
            get_linked_isnis(marc_records, marc_id, isni, linked_ids)
            for linked_id in linked_ids:
                if not linked_id in isnis:
                    for linked_id in linked_ids:
                        linked_record = marc_records[linked_id]
                        for field in linked_record.get_fields('024'):
                            fields.append(field)
                            if 'a' in field and '2' in field:
                                if field['2'] == 'isni':
                                    isnis[linked_id] = field['a']
                                    if linked_id == marc_id and isni != field['a']:
                                        deletable_field = field
            for linked_id in linked_ids:
                if linked_id != marc_id:
                    if linked_id in isnis and marc_id in isnis and isnis[linked_id] == isnis[marc_id]:
                        if marc_id in identifiers and isnis[marc_id] != identifiers[marc_id]['ISNI']:
                            logging.error("ISNI %s changed in record %s to ISNI %s, check linked record %s with same ISNI"
                                            %(isnis[marc_id], marc_id, identifiers[marc_id]['ISNI'], linked_id))
        if deletable_field:
            marc_record = marc_records[marc_id]
            field = Field(tag="001", data=str(marc_record.leader))
            f = mc.create_aleph_seq_field(marc_id, field, leader=True)
            print(f)
            for field in marc_record.get_fields():
                if field == deletable_field:
                    field = Field(
                        tag = '024',
                        indicators = ['7',' '],
                        subfields = [
                            Subfield(code='a', value=identifiers[marc_id]['ISNI']),
                            Subfield(code='2', value='isni'),
                        ])
                f = mc.create_aleph_seq_field(marc_id, field, leader=False)
                print(f)
    for isni in isni_ids:
        if len(isni_ids[isni]) > 1:
            logging.error('Multiple local identifiers %s for ISNI identifier %s'%(isni_ids[isni], isni))
        for marc_id in isni_ids[isni]:
            if marc_id not in marc_records or not '001' in marc_records[marc_id]:
                logging.error("Local id %s for ISNI identifier %s not found"%(marc_id, isni))

def main(args):
    identifiers = {}
    for file_path in args.isni_file:
        identifiers.update(get_identifiers(file_path))
    if args.output_identifiers:
       for id in identifiers:
           print(f'{id}|{identifiers[id]["ISNI"]}')
    if args.handle_marc_records:
        output_aleph_sequential_fields(identifiers, args)

if __name__ == "__main__":
    """
    Script that checks assigned ISNIs from ISNI notification files as defined in
    ISNI technical documentation: https://isni.org/page/technical-documentation/
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--authority_files",
        help="Input path of Aleph sequential file of MARC21 records")
    parser.add_argument("-i", "--isni_file",
        help="Input path of ISNI notification XML file", required=True, action='append')
    parser.add_argument("-m", "--handle_marc_records",
        help="Handle and output updated ISNIs to marc records", action='store_true')
    parser.add_argument("-t", "--output_identifiers",
        help="Output local and ISNI identifiers", action='store_true')
    parser.add_argument("-c", "--config_file_path",
            help="File path for configuration file structured for Python ConfigParser")
    args = parser.parse_args()

    main(args)
