import re
import os
import sys
import logging
import xml.etree.ElementTree as ET

def get_possible_matches(root):
    possible_matches = {}
    for match in root.findall('possibleMatch'): 
        id = None
        for ppn in match.findall('PPN'): 
            id = ppn.text
        possible_matches[id] = {'source ids': []}
        for evaluation_score in match.findall('evaluationScore'): 
            possible_matches[id]['evaluation score'] = evaluation_score.text
        for source in match.findall('source'): 
            possible_matches[id]['source'] = source.text
    return possible_matches

def parse_response_data(root):
    parsed_data = {'errors': []}
    for assigned in root.findall('ISNIAssigned'):
        for isni in assigned.findall('isniUnformatted'): 
            parsed_data['isni'] = isni.text.replace(" ", "")
        possible_matches = get_possible_matches(assigned)
        source_data = []
        for metadata in assigned.findall('ISNIMetadata'):
            for sources in metadata.findall('sources'):
                for code_of_source in sources.findall('codeOfSource'):
                    if code_of_source.text != "NLFIN":
                        for source_identifier in sources.findall('sourceIdentifier'):
                            source_data.append(code_of_source.text)
                            source_data.append(source_identifier.text)
        parsed_data['sources'] = source_data
    for unassigned in root.findall('noISNI'):
        for ppn in unassigned.findall('PPN'):
            parsed_data['ppn'] = ppn.text
        possible_matches = get_possible_matches(unassigned)
        if possible_matches:
            if 'possible matches' in parsed_data:
                parsed_data['possible matches'].update(possible_matches)
            else:
                parsed_data['possible matches'] = possible_matches
        for reason in unassigned.findall('reason'):
            parsed_data['reason'] = reason.text

    return parsed_data

def get_data_from_xml_response_text(response):
    """
    :param response: XML text string from ISNI AtomPub response
    """
    parsed_data = {'errors': []}
    try:
        tree = ET.ElementTree(ET.fromstring(response))
        root = tree.getroot()
        parsed_data = parse_response_data(root)
    except ET.ParseError:
        # in case that ISNI response is error message and not XML
        for line in response.splitlines():
            if line:
                parsed_data['errors'].append(line)

    return parsed_data

def get_response_data_from_xml_file(file_path):

    # Get ISNIs for assigned records and PPNs for records
    # Note: file names should consist of a local identifier (ISNI response does not have local identifiers) 
    if os.path.isdir(file_path):
        file_paths = [os.path.join(path, name) for path, subdirs, files in os.walk(file_path) for name in files]
    elif os.path.isfile(file_path):
        file_paths = [args.input_file]
    else:
        logging.error("Not valid file path: %s"%file_path)
        sys.exit(2)
    contributor_ids = {}
    for file_path in file_paths:
        local_id = file_path
        if "\\" in local_id:
                local_id = local_id.split('\\')[-1]
        if "." in local_id:
            local_id = local_id.split('.')[0]
        if not local_id:
            logging.error("File name %s not properly formatted, should contain "%file_path)
        else:
            parsed_data = {}
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                parse_response_data(root)
            except ET.ParseError:
                parsed_data['error'] = []
                with open(file_path, 'rb') as fh:
                    error_message = fh.read().decode("utf-8") 
                    for line in error_message:
                        if line:
                            parsed_data['error'].append(line)
            contributor_ids[local_id] = parsed_data   
    return contributor_ids   

def get_isni_identifiers(response):
    """
    :param response: XML text string from ISNI AtomPub response
    """
    tree = ET.ElementTree(ET.fromstring(response))
    root = tree.getroot()
    identifiers = {'isni': None, 'ppn': None, 'deprecated isnis': []}
    isni_path = 'ISNIAssigned/isniUnformatted'
    ppn_path = 'noISNI/PPN'
    deprecated_path = 'ISNIAssigned/mergedISNI'
    for isni in root.findall(isni_path):
        identifiers['isni'] = isni.text
    for ppn in root.findall(ppn_path):
        identifiers['ppn'] = ppn.text
    for deprecated_isni in root.findall(deprecated_path):
        identifiers['deprecated isnis'].append(deprecated_isni.text)

    return identifiers

def get_contributor_identifiers(contributor_id):
    """
    returns list of contributor numbers from ISNI "unsuccessful" bulk load response file
    contributor_id: contributor's id in ISNI database (e. g. NLFIN)
    bulkload_file: file location of ISNI XML bulkload file (e. g. bulkload_Unsuccessful.xml)
    """
    contributor_identifiers = {}
    for file_path in file_paths:
        tree = ET.parse(file_path)
        root = tree.getroot()
        for record in root.findall('record'):
            local_identifier = None
            ppn = None
            for url in record.findall('URL'):
                ppn = url.text
            for li in record.findall('localIdentification'):  
                for c in li.findall('contributor'):
                    if contributor_id == c.text:
                
                        for id in li.findall('localIdentifier'):
                            local_identifier = id.text
                            local_identifier = re.sub("[\(].*?[\)]", "", local_identifier)
                            if local_identifier in contributor_identifiers:
                                logging.error("%s has multiple PPNS"%local_identifier)
                                #contributor_identifiers[local_identifier].add(ppn)
                            contributor_identifiers[local_identifier] = {"url": ppn}
    
    return contributor_identifiers
