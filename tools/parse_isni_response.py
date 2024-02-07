import json
from lxml import etree as ET

NAMESPACES = {'srw': 'http://www.loc.gov/zing/srw/'}

def get_number_of_records(response):
    root = ET.fromstring(bytes(response, encoding='latin1'))
    for number in root.findall('srw:numberOfRecords', NAMESPACES):
        if number.text is not None:
            return int(number.text)

def get_response_records(response):
    """
    :param response: XML text string from ISNI AtomPub or SRU API response
    """
    # AtomPub response containing only one respnse record
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    if root.tag == 'responseRecord':
        return [root]
    response_records = []
    # SRU API response containing one or more response records
    for records in root.findall('srw:records', NAMESPACES):
        for record in records.findall('srw:record', NAMESPACES):
            for record_data in record.findall('srw:recordData', NAMESPACES):
                for response_record in record_data.findall('responseRecord'):
                    response_records.append(response_record)
    return response_records

def add_values_to_list(valuelist, values, source_code):
    """
    Adds a dict with key 'values' to list if list does not contain values. If values exits, just add source code to list. 
    """
    value_data = {'values': values, 'sources': [source_code]}
    if not any(values == value_obj['values'] for value_obj in valuelist):
        valuelist.append(value_data)
    else:
        for value_obj in valuelist:
            if values == value_obj['values']:
                if source_code not in value_obj['sources']:
                    value_obj['sources'].append(source_code)

def get_status(record):
    """
    Gets ISNI status from ISNI record. In AtomPub response unassigned records have status noISNI
    """
    if record.findall('ISNIAssigned'):
        return 'ISNIAssigned'
    if record.findall('ISNINotAssigned'):
        return 'ISNINotAssigned'
    if record.findall('noISNI'):
        return 'noISNI'

def get_reason(record, status):
    for reason in record.findall(status + '/reason'):
        return reason.text

def get_identifier(record, status):
    for isni_data in record.findall(status):
        for identifier in isni_data.findall('isniUnformatted'):
            return {'type': 'isni', 'identifier': identifier.text}
        for identifier in isni_data.findall('PPN'):
            return {'type': 'ppn', 'identifier': identifier.text}

def get_deprecated_isnis(record, status):
    deprecated_isnis = []
    for deprecated_isni in record.findall(status + '/mergedISNI'):
        deprecated_isnis.append(deprecated_isni.text)
    
    return deprecated_isnis

def get_possible_matches(record, status):
    """
    Gets possible matches from AtomPub response
    """
    possible_matches = []
    for pm in record.findall(status + '/possibleMatch'):
        values = {}
        for ppn in pm.findall('PPN'):
            values['ppn'] = ppn.text
        for score in pm.findall('evaluationScore'):
            values['evaluationScore'] = score.text
        possible_matches.append(values)
    
    return possible_matches

def get_identity_type(record, status):
    for data in record.findall(status + '/ISNIMetadata/identity'):
        for elem in data:
            if elem.tag:
                return elem.tag

def get_source_identifiers(record, status):
    sources = {}
    for data in record.findall(status + '/ISNIMetadata/sources'):
        source_code = None
        identifier = None
        for code_of_source in data.findall('codeOfSource'):
            source_code = code_of_source.text
        for source_identifier in data.findall('sourceIdentifier'):
            identifier = source_identifier.text
        sources[source_code] = identifier

    return sources

def get_external_information(record, status):
    uris = []
    for info in record.findall(status + '/ISNIMetadata/externalInformation'):
        for uri in info.findall('URI'):
            uris.append(uri.text)

    return uris

def get_other_identifiers(record, status):
    other_identifiers = []
    for other_identifier in record.findall(status + '/ISNIMetadata/otherIdentifierOfIdentity'):
        identifier_type = None
        identifier = None
        source_code = None
        for identifier_type in other_identifier.findall('type'):
            identifier_type = identifier_type.text
        for identifier in other_identifier.findall('identifier'):
            identifier = identifier.text
        for source in other_identifier.findall('source'):
            source_code = source.text
        values = {'type': identifier_type, 'identifier': identifier}
        add_values_to_list(other_identifiers, values, source_code)

    return other_identifiers

def get_name_attributes(name, keys):
    name_data = {}
    for attrib in keys:
        for elem in name.findall(attrib):
            if attrib == 'subdivisionName':
                if not attrib in name_data:
                    name_data[attrib] = []
                name_data[attrib].append(elem.text)
            else:
                name_data[attrib] = elem.text
    return name_data

def get_name_variants(record, status, identity_type):
    names = {'main names': [], 'variant names': []}
    path = status + '/ISNIMetadata/identity/' + identity_type + '/'
    if identity_type == 'personOrFiction':
        name_types = ['personalName', 'personalNameVariant']
    elif identity_type == 'organisation':
        name_types = ['organisationName', 'organisationNameVariant']
    for name_type in name_types:
        name_path = path + name_type
        for variant_name in record.findall(name_path):
            keys = []
            name = {}
            source_code = None
            if identity_type == 'personOrFiction':
                keys = ['surname', 'forename'] 
            elif identity_type == 'organisation':
                keys = ['mainName', 'subdivisionName']
            name['values'] = get_name_attributes(variant_name, keys)
            for source in variant_name.findall('source'):
                source_code = source.text
            if name_type in ['personalName', 'organisationName']:
                add_values_to_list(names['main names'], name['values'], source_code)
            if name_type in ['personalNameVariant', 'organisationNameVariant']:
                add_values_to_list(names['variant names'], name['values'], source_code)

    return names

def get_related_identitites(record, status, identity_type):
    related_identities = []
    path = status + '/ISNIMetadata/identity/' + identity_type + '/'
    related_types = ['isRelatedPerson', 'isRelatedOrganisation']
    for related_type in related_types:
        name_path = path + related_type
        for related in record.findall(name_path):
            keys = []
            source_code = None
            if related_type == 'isRelatedPerson':
                related_identity_type = 'personOrFiction'
                keys = ['surname', 'forename'] 
            if related_type == 'isRelatedOrganisation':
                related_identity_type = 'organisation'
                keys = ['mainName', 'subdivisionName']
            related_identity = {'values': {'identity type': related_identity_type}}
            related_identity['values'].update(get_name_attributes(related, keys))
            isni = None
            for identifier in related.findall('ISNI'):
                isni = identifier.text
            related_identity['values']['ISNI'] = isni
            for source in related.findall('source'):
                source_code = source.text
            add_values_to_list(related_identities, related_identity['values'], source_code)

    return related_identities

def get_additional_information(record, status, identity_type):
    path = status + '/ISNIMetadata/identity/' + identity_type + '/additionalInformation'
    additional_info = {}
    for info in record.findall(path):
        for elem in info:
            source_codes = []
            if elem.tag not in additional_info:
                additional_info[elem.tag] = []
            for data in info.findall(elem.tag):
                value = elem.text
            if elem.tag == 'countriesAssociated':
                for county_code in elem.findall('countryCode'):
                    value = county_code.text
            if elem.tag == 'location':
                for county_code in elem.findall('countryCode'):
                    value = county_code.text
                for source_code in elem.findall('source'):
                    source_codes.append(source_code.text)
            elif elem.tag != 'nationality':
                source_codes.append(json.loads(str(elem.attrib).replace('\'', '"'))['source'])
            for source_code in source_codes:
                add_values_to_list(additional_info[elem.tag], value, source_code)

    return additional_info

def dictify_xml(response):    
    response_records = get_response_records(response)
    isnis = []
    for record in response_records:
        status = get_status(record)
        isni_record = {}
        identifier_obj = get_identifier(record, status)
        if status == 'noISNI':
            isni_record['reason'] = get_reason(record, status)
        if identifier_obj:
            isni_record[identifier_obj['type']] = identifier_obj['identifier']
        isni_record['possible matches'] = get_possible_matches(record, status)
        isni_record['deprecated isnis'] = get_deprecated_isnis(record, status)
        identity_type = get_identity_type(record, status)
        if identity_type:
            isni_record['identity type'] = identity_type
            isni_record['uris'] = get_external_information(record, status)
            isni_record['other identifiers'] = get_other_identifiers(record, status)
            isni_record.update(get_name_variants(record, status, identity_type))
            isni_record['related identities'] = get_related_identitites(record, status, identity_type)
            isni_record.update(get_additional_information(record, status, identity_type))
            isni_record['related identities'] = get_name_variants(record, status, identity_type)
            isni_record['sources'] = get_source_identifiers(record, status)
        isnis.append(isni_record)

    return isnis