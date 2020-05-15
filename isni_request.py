from xml.dom.minidom import parseString
import xml.etree.cElementTree as ET
import logging
import os.path

"""
Data collected from a record and used in create_xml function:

mergeToISNI (ISNI id from the record, if merge instruction needed)
identifier (requestor's own identifier)
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
countriesAssociated (Note: location of organisation or nationality are possible to add to an ISNI request)
resource = [{'title',
             'creationClass',
             'creationRole',
             'title',
             'publisher',
             'date',
             'identifiers': {'ISRC': [], 'ISBN: [], 'ISSN: []}

}]
"""

def create_name_subelements(root, name_data):     
    for nd in name_data:
        if name_data[nd]:
            create_subelement(root, name_data, nd)
    
def create_subelement(root, dicta, key):
    """
    :param root: a parent of the subelement
    :param dicta: dict that contains the name of the subelement as a key
    :param key: key to access the subelement data in dicta
    """
    if type(dicta[key]) == list:
        for d in dicta[key]:
            sub_element = ET.SubElement(root, key)
            sub_element.text = d
    elif type(dicta[key]) == str:
        sub_element = ET.SubElement(root, key)
        sub_element.text = dicta[key]

def create_resources(root, resource_data):
    for r in resource_data:
        resource = ET.SubElement(root, 'resource')
        if r['creationClass']:
            create_subelement(resource, r, 'creationClass')
        if r['creationRole']:
            create_subelement(resource, r, 'creationRole')        
        titleOfWork = ET.SubElement(resource, 'titleOfWork')
        create_subelement(titleOfWork, r, 'title')
        
        if 'ISRC' in r['identifiers']:
            for identifier in r['identifiers']['ISRC']:
                isrc = ET.SubElement(titleOfWork, 'isrc')
                isrc.text = identifier
        if r['publisher']:
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

def create_xml(record_id, record_data):
    request = ET.Element("Request")
    
    try:
        identityInformation = ET.SubElement(request, 'identityInformation')
        requestorIdentifierOfIdentity = ET.SubElement(identityInformation, 'requestorIdentifierOfIdentity')
        create_subelement(requestorIdentifierOfIdentity, record_data, 'identifier')
        if 'otherIdentifierOfIdentity' in record_data:
            for oid in record_data['otherIdentifierOfIdentity']:
                otherIdentifierOfIdentity = ET.SubElement(identityInformation, 'otherIdentifierOfIdentity')
                for element in oid:
                    create_subelement(otherIdentifierOfIdentity, oid, element)
        identity = ET.SubElement(identityInformation, 'identity')
        
        if record_data['identityType'] == 'personOrFiction':
            identity_data = record_data['personalName']
            personOrFiction = ET.SubElement(identity, 'personOrFiction')
            personalName = ET.SubElement(personOrFiction, 'personalName')
            create_name_subelements(personalName, identity_data)
            identity_elements = ['gender', 'birthDate', 'deathDate', 'dateType']
            for elem in identity_elements:
                if elem in record_data:
                    if record_data[elem]:
                        create_subelement(personOrFiction, record_data, elem)
            #TODO: nationality, contributedTo and instrumentAndVoice possible here
            if record_data['resource']:
                create_resources(personOrFiction, record_data['resource'])
            if record_data['personalNameVariant']:
                for name in record_data['personalNameVariant']:
                    personalNameVariant = ET.SubElement(personOrFiction, 'personalNameVariant')
                    create_name_subelements(personalNameVariant, name)
        elif record_data['identityType'] == 'organisation':
            identity_data = record_data['organisationName']
            organisation = ET.SubElement(identity, 'organisation')
            #create_subelement(organisation, identity_data, 'organisationType')
            organisationName = ET.SubElement(organisation, 'organisationName') 
            create_name_subelements(organisationName, identity_data)
            usage_dates = ['usageDateFrom', 'usageDateTo']
            for usage_date in usage_dates:
                if usage_date in identity_data:
                    if identity_data[usage_date]:
                        create_subelement(organisation, identity_data, usage_date)
            #TODO: location of HQ here?
            if record_data['resource']:
                create_resources(organisation, record_data['resource'])
            if record_data['organisationNameVariant']:
                for name in record_data['organisationNameVariant']:
                    organisationNameVariant = ET.SubElement(organisation, 'organisationNameVariant')
                    create_name_subelements(organisationNameVariant, name)

        if record_data['languageOfIdentity']:
            for code in record_data['languageOfIdentity']:
                languageOfIdentity = ET.SubElement(identityInformation, 'languageOfIdentity')
                languageOfIdentity.text = code
                
        if record_data['countriesAssociated']:
            for cc in record_data['countriesAssociated']:
                countriesAssociated = ET.SubElement(identityInformation, 'countriesAssociated')
                countryCode = ET.SubElement(countriesAssociated, 'countryCode')
                countryCode.text = cc
                #TODO: regionOrState and city also possible to add here        
        if record_data['URI']:
            #TODO: source and information also possible in this element
            for record_uri in record_data['URI']:
                externalInformation = ET.SubElement(identityInformation, 'externalInformation')
                uri = ET.SubElement(externalInformation, 'URI')
                uri.text = record_uri
        if 'isRelated' in record_data:
            for relation in record_data['isRelated']:
                isRelated = ET.SubElement(request, 'isRelated')
                isRelated.set("identityType", relation['identityType'])
                create_subelement(isRelated, relation, 'relationType')
                relationName = ET.SubElement(isRelated, 'relationName')
                if 'ISNI' in relation:
                    create_subelement(relationName, relation, 'ISNI')
                if relation['personalName']:
                    personalName = ET.SubElement(relationName, 'personalName')
                    create_name_subelements(personalName, relation['personalName'])
                elif relation['organisationName']:
                    organisationName = ET.SubElement(relationName, 'organisationName')
                    create_name_subelements(organisationName, relation['organisationName'])
                if relation['startDateOfRelationship']:
                    create_subelement(isRelated, relation, 'startDateOfRelationship')
                if relation['endDateOfRelationship']:
                    create_subelement(isRelated, relation, 'endDateOfRelationship')
        """
        mergeToISNI is not possible in ISNI batch loads: "Note: Not supported for offline batch import"
        mergeInstruction values:
            M - request to merge
            P - used rarely - when manual review needed
            N - used rarely - when the target database record contains a possible match field that is false. 
        if 'mergeToISNI' in record_data:
            create_subelement(request, paths['mergeToISNI'], record_data['mergeToISNI'])
        """

    except KeyError as e:
        raise ValueError("Data %s missing from record %s"%(e.args[0], record_id))
        
    xml = parseString(ET.tostring(request, "utf-8")).toprettyxml()
    return(xml)
    

    