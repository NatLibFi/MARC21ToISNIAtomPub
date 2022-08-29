from xml.dom.minidom import parseString
import xml.etree.cElementTree as ET
import logging
import os.path

"""
Data collected from a record and used in create_xml function:

identifier (requestor's own identifier)
otherIdentifierOfIdentity = [{'identifier', 'type'}]
identityType = 'personalName' / 'organisationName'
isRelated = [{'identityType = 'organisation' / 'personOrFiction',
              'relationType' ("consult ISNI data element values.doc"],
              'ISNI',
              'startDateOfRelationship',
              'endDateOfRelationship',
              'personalName' / 'organisationName'}}
birthDate  
deathDate    
   
personalName = {'nameUse' (default value "public"),
                'surname',
                'forename',
                'numeration',
                'marcDate'}

organisationName = {'mainName',
                    'subdivisionName']
personalNameVariant = [personalName, personalName]                    
organisationNameVariant = [organisationName, organisationName]           
organisationType
languageOfIdentity
URI 
countryCode (country code of HQ of an organisation)
countriesAssociated (Note: country code of nationality are possible to add to an ISNI request)
resource = [{'title',
             'creationClass',
             'creationRole',
             'title',
             'publisher',
             'date',
             'identifiers': {'ISRC': [], 'ISBN: [], 'ISSN: []}

}]
"""

def create_person_name_subelements(root, name_data):
    """
    helper function to create sublements to AtomPub XML
    """
    element_order = ['nameUse', 'surname', 'forename', 'numeration', 'nameTitle', 'marcDate', 'languageOfName', 'script']
    if name_data:
        for elem in element_order:
            if name_data.get(elem):
                create_subelement(root, name_data, elem)

def create_organisation_name_subelements(root, name_data):
    """
    helper function to create sublements to AtomPub XML
    """
    element_order = ['mainName', 'subdivisionName']
    if name_data:
        for elem in element_order:
            if name_data.get(elem):
                create_subelement(root, name_data, elem)

def create_subelement(root, dicta, key):
    """
    helper function to create sublements to AtomPub XML
    :param root: a parent of the subelement
    :param dicta: dict that contains the name of the subelement as a key
    :param key: key to access the subelement data in dicta
    """
    if dicta.get(key):
        if type(dicta[key]) == list:
            for d in dicta[key]:
                sub_element = ET.SubElement(root, key)
                sub_element.text = d
        elif type(dicta[key]) == str:
            sub_element = ET.SubElement(root, key)
            sub_element.text = dicta[key]

def create_resources(root, resource_data):
    """
    creates resources element to AtomPub XML
    :param root: person or organisation element in ISNI AtomPub XML
    :param resource_data: list of dicts containing title data
    """
    for r in resource_data:
        if r.get('title'):
            resource = ET.SubElement(root, 'resource')
            create_subelement(resource, r, 'creationClass')
            create_subelement(resource, r, 'creationRole')
            titleOfWork = ET.SubElement(resource, 'titleOfWork')
            create_subelement(titleOfWork, r, 'title')
        
        if r['identifiers'].get('ISRC'):
            for identifier in r['identifiers']['ISRC']:
                isrc = ET.SubElement(titleOfWork, 'isrc')
                isrc.text = identifier
        if r.get('publisher'):
            imprint = ET.SubElement(titleOfWork, 'imprint')
            create_subelement(imprint, r, 'publisher')
            create_subelement(imprint, r, 'date')
        
        for identifier_type in r['identifiers']:
            if not identifier_type == 'ISRC':
                for value in r['identifiers'][identifier_type]:
                    identifier = ET.SubElement(titleOfWork, 'identifier')
                    identifierValue = ET.SubElement(identifier, 'identifierValue')
                    identifierValue.text = value
                    identifierType = ET.SubElement(identifier, 'identifierType')
                    identifierType.text = identifier_type

def validate_isni_id(isni_id):
    """Validate ISNI identifier in case of typos"""
    isni_id = isni_id.replace(' ', '')
    if len(isni_id) == 16:
        return {'identifier': isni_id, 'type': 'ISNI'}
    elif len(isni_id) == 9:
        return {'identifier': isni_id, 'type': 'PPN'}
    else:
        logging.error('The length of ISNI identifier %s is not 9 or 16 characters'%isni_id)  

def create_xml(record_data, instruction=None, isni_identifiers=[]):
    """
    Creates ISNI AtomPub XML
    :param record_data: record data in dict object as described in commented section above
    :param instruction: instructions merge or isNot for adding otherIdentifierOfIdentity or isNot element to request
    :param isni_identifiers: ISNI identifiers for ISNI records that will be merged to local record or dissociated from local record
    """
    request = ET.Element("Request")
    try:
        identityInformation = ET.SubElement(request, 'identityInformation')
        requestorIdentifierOfIdentity = ET.SubElement(identityInformation, 'requestorIdentifierOfIdentity')
        create_subelement(requestorIdentifierOfIdentity, record_data, 'identifier')
        isni_id = None
        local_isni = None
        if instruction == "merge" and isni_identifiers:
            isni_id = validate_isni_id(isni_identifiers[0])
        if record_data.get('ISNI'):
            local_isni = validate_isni_id(record_data['ISNI'])
        if isni_id and local_isni:
            if isni_id['identifier'] != local_isni['identifier']:
                logging.error("ISNI identifier %s in merge instruction differs from local ISNI %s"%(isni_id, local_isni))
                return
        elif local_isni:
            isni_id = local_isni
        if isni_id:
            record_data['otherIdentifierOfIdentity'].append({'identifier': isni_id['identifier'], 'type': isni_id['type']})
        if record_data.get('otherIdentifierOfIdentity'):
            for oid in record_data['otherIdentifierOfIdentity']:
                otherIdentifierOfIdentity = ET.SubElement(identityInformation, 'otherIdentifierOfIdentity')
                for element in oid:
                    create_subelement(otherIdentifierOfIdentity, oid, element)
        identity = ET.SubElement(identityInformation, 'identity')
        
        if record_data['identityType'] == 'personOrFiction':
            identity_data = record_data['personalName']
            personOrFiction = ET.SubElement(identity, 'personOrFiction')
            personalName = ET.SubElement(personOrFiction, 'personalName')
            create_person_name_subelements(personalName, identity_data)
            identity_elements = ['gender', 'birthDate', 'deathDate', 'dateType']
            for elem in identity_elements:
                create_subelement(personOrFiction, record_data, elem)
            #TODO: nationality, contributedTo and instrumentAndVoice possible here
            if record_data.get('resource'):
                create_resources(personOrFiction, record_data['resource'])
            if record_data.get('personalNameVariant'):
                for name in record_data['personalNameVariant']:
                    personalNameVariant = ET.SubElement(personOrFiction, 'personalNameVariant')
                    create_person_name_subelements(personalNameVariant, name)
        elif record_data['identityType'] == 'organisation': 
            organisation = ET.SubElement(identity, 'organisation')
            create_subelement(organisation, record_data, 'organisationType')
            identity_data = record_data['organisationName']
            organisationName = ET.SubElement(organisation, 'organisationName') 
            create_organisation_name_subelements(organisationName, identity_data)
            usage_dates = ['usageDateFrom', 'usageDateTo']
            for usage_date in usage_dates:
                if usage_date in record_data:
                    if record_data[usage_date]:
                        create_subelement(organisation, record_data, usage_date)
            if record_data.get('countryCode'):
                location = ET.SubElement(organisation, 'location')
                create_subelement(location, record_data, 'countryCode')
            if record_data.get('resource'):
                create_resources(organisation, record_data['resource'])

            if record_data.get('organisationNameVariant'):
                for name in record_data['organisationNameVariant']:
                    organisationNameVariant = ET.SubElement(organisation, 'organisationNameVariant')
                    create_organisation_name_subelements(organisationNameVariant, name)

        if record_data.get('languageOfIdentity'):
            for code in record_data['languageOfIdentity']:
                languageOfIdentity = ET.SubElement(identityInformation, 'languageOfIdentity')
                languageOfIdentity.text = code
                
        if record_data.get('countriesAssociated'):
            for cc in record_data['countriesAssociated']:
                countriesAssociated = ET.SubElement(identityInformation, 'countriesAssociated')
                countryCode = ET.SubElement(countriesAssociated, 'countryCode')
                countryCode.text = cc
                #TODO: regionOrState and city also possible to add here        
        if record_data.get('URI'):
            #TODO: source and information also possible in this element
            for record_uri in record_data['URI']:
                externalInformation = ET.SubElement(identityInformation, 'externalInformation')
                uri = ET.SubElement(externalInformation, 'URI')
                uri.text = record_uri     
        if (instruction == 'isNot' and isni_identifiers) or 'isNot' in record_data:
            if record_data['isNot']:
                if not isni_identifiers:
                    isni_identifiers = []
                for isni_id in record_data['isNot']:
                    isni_identifiers.append(isni_id)
            for isni_id in isni_identifiers:
                isni_id = validate_isni_id(isni_id)
                if isni_id:
                    isNot = ET.SubElement(request, 'isNot')
                    isNot.set("identityType", record_data['identityType'])
                    relationName = ET.SubElement(isNot, 'relationName')
                    identifier = ET.SubElement(relationName, isni_id['type'])
                    identifier.text = isni_id['identifier']
        if record_data.get('isRelated'):
            for relation in record_data['isRelated']:
                isRelated = ET.SubElement(request, 'isRelated')
                isRelated.set("identityType", relation['identityType'])
                create_subelement(isRelated, relation, 'relationType')
                relationName = ET.SubElement(isRelated, 'relationName')
                create_subelement(relationName, relation, 'ISNI')
                if relation.get('personalName'):
                    personalName = ET.SubElement(relationName, 'personalName')
                    create_person_name_subelements(personalName, relation['personalName'])
                elif relation.get('organisationName'):
                    organisationName = ET.SubElement(relationName, 'organisationName')
                    create_organisation_name_subelements(organisationName, relation['organisationName'])
                create_subelement(isRelated, relation, 'startDateOfRelationship')
                create_subelement(isRelated, relation, 'endDateOfRelationship')

    except KeyError as e:
        raise ValueError("Data %s missing from record %s"%(e.args[0], record_data['identifier']))
    xml = parseString(ET.tostring(request, "utf-8")).toprettyxml()
    return(xml)
    

    