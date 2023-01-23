import logging
from resource_list import ResourceList
from term_encoder import TermEncoder
from validators import Validator
from tools import api_query
from tools import parse_sru_response
from tools import parse_oai_response
from pymarc import MARCReader
from tools import aleph_seq_reader  
import copy
import io
import json
import re
import sys
import configparser

class MARC21Converter:
    """
        A class to collect data from mrc binary files to ISNI Atom Pub XML format.
    """
    def __init__(self):
        self.term_encoder = TermEncoder()
        self.validator = Validator()
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.records = {}
        self.resources = None
        self.sru_bib_query = None

    def get_linked_records(self, record, marc_records, identifiers):
        """
        fetch recursively all records that are linked to organisation authority record with MARC field 510
        :param record: MARC21 record of an organisation
        :param records: list of recursively gathered MARC21 records
        :param identifiers: list of identifiers of linked organisation records
        """
        tags = ['500', '510']
        for tag in tags:
            for field in record.get_fields(tag):
                linked_identifier = None
                if field['0']:
                    linked_identifier = re.sub("[\(].*?[\)]", "", field['0'])
                    if linked_identifier not in identifiers:
                        identifiers.add(linked_identifier)
                        parameters = {'doc_num': linked_identifier}
                        response = self.oai_x_query.api_search(parameters=parameters)
                        marc_record = parse_oai_response.get_records(response)[0]
                        #if not record in marc_records:
                        marc_records.append(marc_record)
                        self.get_linked_records(marc_record, marc_records, identifiers)

    def get_authority_data(self, args, requested_ids=set()):
        """
        :param args: parameters that are passed to converter as command line arguments
        :param requested_ids: a set of local identifiers to be converted into ISNI request
        """
        if args.resource_files:
            self.resources = ResourceList(args.resource_files, args.format).titles
        else:
            self.resource_list = ResourceList()
            self.resources = self.resource_list.titles
            section = self.config['BIB SRU API']                      
            self.sru_bib_query = api_query.APIQuery(config_section=section)
        reader = []
        # for record identifiers of new records if keyword arg created_after is used (titles for modified records are not requested)
        current_ids = []
        # identifiers of identities linked to requested identities
        linked_ids = []
        deletable_identities = set()
        # these two sets for the case when requested_ids parameter is used,
        # id not in requested_ids but merged with requested_ids
        related_identities = set()
        not_requested_ids = set()

        if not args.authority_files:
            logging.info("Requesting authority records with API")
            identifiers = []
            if requested_ids:
                for identifier in requested_ids: 
                    identifiers.append(identifier)
            elif args.modified_after or args.created_after or args.until:
                section = self.config['AUT OAI-PMH API']                      
                oai_pmh_query =  api_query.APIQuery(config_section=section)
                query_parameters = {"metadataPrefix": "melinda_marc"}
                if args.modified_after:
                    query_parameters['from'] = args.modified_after
                elif args.created_after:
                    query_parameters['from'] = args.created_after
                if args.until:
                    query_parameters['until'] = args.until
                response = oai_pmh_query.api_search(parameters=query_parameters)
                identifiers = parse_oai_response.get_identifiers(response)
                token = parse_oai_response.get_resumption_token(response)
                if token:
                    while True:
                        answer = input("Over 1000 updated records. Resume OAI-PMH requesting (Y/N)?")
                        if answer.lower() == "y":
                            break
                        if answer.lower() == "n":
                            sys.exit(2)
                    while token:
                        token_parameters = {'resumptionToken': token}
                        response = oai_pmh_query.api_search(token_parameters=token_parameters)
                        identifiers.extend(parse_oai_response.get_identifiers(response))
                        token = parse_oai_response.get_resumption_token(response)
            else:
                logging.error("Command line argument modified_after required if authority file not given")
                sys.exit(2)
            section = self.config['AUT X API']                      
            self.oai_x_query = api_query.APIQuery(config_section=section)                                           
            requested_ids.update(identifiers)
            for identifier in identifiers:
                parameters = {'doc_num': identifier}
                response = self.oai_x_query.api_search(parameters=parameters)
                marc_records = []
                record = parse_oai_response.get_records(response)[0]
                if not record in marc_records:
                    marc_records.append(record)
                if record['001']:
                    linked_identifiers = {record['001'].data}
                    self.get_linked_records(record, marc_records, linked_identifiers)
                    linked_ids.extend(linked_identifiers - {record['001'].data})
                    for mr in marc_records:
                        if not any(mr['001'].data == rec['001'].data for rec in reader):
                            reader.append(mr)
                    requested_ids.update(linked_identifiers - {record['001'].data})

        self.max_number_of_titles = int(self.config['SETTINGS'].get('max_titles'))
        if args.identity_types == "persons":
            convertible_fields = ['100']
        elif args.identity_types == "organisations":
            convertible_fields = ['110']
        else:
            convertible_fields = ['100', '110']
        isnis = {}
        identities = {}
        if not reader:
            if args.format == "marc21":
                reader = MARCReader(open(args.authority_files, 'rb'), to_unicode=True)
            elif args.format == "alephseq":                     
                reader = aleph_seq_reader.AlephSeqReader(open(args.authority_files, 'r', encoding="utf-8"))
            else:
                logging.error("Not valid format to convert from: "%args.format)
                sys.exit(2)

        record = ""
        while record is not None:
            record_id = None
            if args.authority_files:
                try:
                    record = next(reader, None)    
                except Exception as e:
                    logging.exception(e) 
            else:
                if reader:
                    record = reader.pop()
                else:
                    record = None
            if record:
                if record['001']:
                    record_id = record['001'].data
                    if args.authority_files:
                        requested_ids.add(record_id)
            if not record_id or not any(record[f] for f in convertible_fields):
                if record_id:
                    requested_ids.remove(record_id)
                continue
            removable = False
            for field in record.get_fields('075'):
                for sf in field.get_subfields('a'):
                    if sf == "ei-RDA-entiteetti":
                        removable = True
            for field in record.get_fields('STA'):
                for sf in field.get_subfields('a'):
                    if sf in ["TEST", "DELETED"]:
                        removable = True
            if removable:
                requested_ids.remove(record_id)
                continue
            if requested_ids and record_id not in requested_ids:
                related_identities.add(record_id)
                deletable_identities.add(record_id)
            self.records[record_id] = record
            for field in record.get_fields('983'):
                for sf in field.get_subfields('a'): 
                    if sf == "ei-isni-loadi-ed":
                        deletable_identities.add(record_id)
                        not_requested_ids.add(record_id)
            identities[record_id] = {}
            identity = identities[record_id]
            identity['modification date'] = None
            identity['creation date'] = None
            # get cataloging identifiers whose modification to records are neglected
            try:
                self.cataloguers = json.loads(self.config['SETTINGS'].get('cataloguers'))
            except json.decoder.JSONDecodeError as e:
                logging.error("Parameters %s malformatted in config.ini"%self.config['SETTINGS'].get('cataloguers'))
                logging.error(e)
                sys.exit(2)
            for field in record.get_fields("CAT"):
                cataloguer = field['a']
                if cataloguer not in self.cataloguers:
                    for sf in field.get_subfields('c'):
                        formatted_date = sf[:4] + "-" + sf[4:6] + "-" + sf[6:8]
                        if not identity['creation date']:
                            identity['creation date'] = formatted_date
                        identity['modification date'] = formatted_date
            if args.created_after:
                if identity['creation date'] >= args.created_after:
                    if not args.until or identity['creation date'] < args.until:
                        current_ids.append(record_id)
            if args.modified_after:
                if identity['modification date'] >= args.modified_after:
                    if not args.until or identity['modification date'] < args.until:
                        current_ids.append(record_id)
            if args.identifier:
                identity['identifier'] = "(" + args.identifier + ")" + record_id
            else:
                identity['identifier'] = record_id
            identity['isRelated'] = self.get_related_names(record)
            if record['100']:
                identity['identityType'] = 'personOrFiction'
                personal_name = self.get_personal_name(record_id, record['100'])      

                if not personal_name:
                    del(identities[record_id])
                else:
                    identity['personalName'] = personal_name
                    identity['personalNameVariant'] = []
                    for field in record.get_fields('400'):
                        variant = self.get_personal_name(record_id, field)
                        is_related_name = False
                        for sf in field.get_subfields('4'):
                            if sf in ['toni', 'pseu']:
                                is_related_name = True
                        if is_related_name:
                            if variant:
                                related_person = {
                                    "identifier": None,
                                    "identityType": 'personOrFiction',
                                    "organisationName": None,
                                    "personalName": variant,
                                    "startDateOfRelationship": None,
                                    "endDateOfRelationship": None
                                }
                                if sf == 'toni':
                                    related_person['relationType'] = 'real name'
                                elif sf == 'pseu':
                                    related_person['relationType'] = 'pseud'
                                identity['isRelated'].append(related_person)
                        else:
                            if variant:
                                identity['personalNameVariant'].append(variant)
                    fuller_names = self.get_fuller_names(record)
                    for fn in fuller_names:
                        # check for duplicate names:
                        if not any(pnv['surname'] == fn['surname'] and pnv['forename'] == fn['forename'] for pnv in identity['personalNameVariant']):
                            identity['personalNameVariant'].append({'nameUse': 'public', 'surname': fn['surname'], 'forename': fn['forename']})
            elif record['110']:                
                identity['identityType'] = 'organisation'
                organisation_name = self.get_organisation_name(record['110'], record)
                if not organisation_name:
                    del(identities[record_id])
                else:
                    identity['organisationName'] = organisation_name
                    identity['organisationNameVariant'] = []
                    for field in record.get_fields('410'):
                        variant = self.get_organisation_name(field, record)
                        if variant:
                            identity['organisationNameVariant'].append(variant)
                    identity['organisationType'] = self.get_organisation_type(record)   
            if record_id in identities:
                identifiers = {}
                identity['ISNI'] = None
                identity['otherIdentifierOfIdentity'] = []
                for field in record.get_fields('024'):
                    identifier_type = None
                    identifier = None
                    if field['a'] and field['2']:
                        identifier = field['a']
                        if field['2'] in ["viaf", "orcid", "wikidata"]:
                            identifier_type = field['2'].upper()     
                            if field['2'] == "orcid":
                                identifier = self.validator.check_ORCID(field['a'])
                                if not identifier:
                                    logging.error("ORCID %s: invalid in record %s"%(field['a'], record_id))
                            elif field['2'] == "wikidata":               
                                identifier = identifier.replace('https://www.wikidata.org/wiki/', '')
                            if identifier:
                                identifiers[identifier_type] = identifier
                        elif field['2'] == "isni":
                            identity['ISNI'] = identifier
                            isnis[record_id] = identifier
                for identifier_type in identifiers:
                    identity['otherIdentifierOfIdentity'].append({'identifier': identifiers[identifier_type], 'type': identifier_type})
                
                for field in record.get_fields('046'):
                    # NOTE: it is assumed that only one MARC21 field for dates is used
                    dates = self.get_dates(field, identity['identityType'])
                    if dates:
                        if identity['identityType'] == 'personOrFiction':
                            identity['birthDate'] = dates['birthDate'] 
                            identity['deathDate'] = dates['deathDate']
                            identity['dateType'] = dates['dateType']
                        if identity['identityType'] == 'organisation':
                            identity['usageDateFrom'] = dates['usageDateFrom'] 
                            identity['usageDateTo'] = dates['usageDateTo']

                language_codes = []   
                for field in record.get_fields('377'):
                    for sf in field.get_subfields("a"):
                        if self.validator.valid_language_code(sf):
                            language_codes.append(sf)
                        else:    
                            logging.error("%s: wrong language code in field: %s"%(record_id, field))
                identity['languageOfIdentity'] = language_codes

                country_codes = []
                identity['countriesAssociated'] = []
                identity['countryCode'] = None
                for field in record.get_fields("043"):
                    for sf in field.get_subfields("c"):
                        if self.validator.valid_country_code(sf):
                            country_codes.append(sf)

                if any(cc == "AX" for cc in country_codes):
                    identity['countryCode'] = "AX"
                    for cc in country_codes:
                        if cc != "AX":
                            identity['countriesAssociated'].append(cc)
                elif len(country_codes) == 1:
                    identity['countryCode'] = country_codes[0]
                elif len(country_codes) > 1:
                    identity['countryCode'] = country_codes[0]
                    identity['countriesAssociated'] = country_codes[1:]

                uris = []
                for field in record.get_fields("670"):
                    if field['a']:
                        if not field['a'].lower().startswith("väittelijän"):
                            for sf in field.get_subfields("u"):
                                uris.append(sf) 
                identity['URI'] = uris
                identity['resource'] = []
                # get resource information
                for field in record.get_fields('670'):
                    resource = {}
                    for sf in field.get_subfields('a'):
                        if "ENNAKKOTIEDOT-ISNI:" in sf and "(ISBN:" in sf:
                            sf = sf.split("(ISBN:")
                            title = sf[0].replace("ENNAKKOTIEDOT-ISNI:", "").strip()
                            isbn = sf[1].replace("(ISBN:", "")
                            if isbn.endswith(")"):
                                resource['identifiers'] = {'ISBN': [isbn[:-1].strip()]}
                                resource['title'] = title
                                resource['date'] = None
                                resource['language'] = None
                                resource['role'] = 'author'
                            else:
                                logging.error("Field %s malformatted in record: %s"%(field, record_id))
                    if resource:    
                        resource['creationClass'] = None
                        resource['creationRole'] = 'aut'
                        resource['publisher'] = None
                        if not record_id in self.resources:
                            self.resources[record_id] = []
                        self.resources[record_id].append(resource)

        merged_ids = []
        merged_id_clusters = []
        mergeable_relations = ["supersedes", "isSupersededBy"]
        for record_id in identities:
            if record_id not in merged_ids and identities[record_id]['identityType'] == 'organisation':
                ids = [record_id]
                for idx, related_name in enumerate(identities[record_id]['isRelated']):
                    relationType = related_name['relationType']
                    if relationType in mergeable_relations:
                        # in case MARC field 510 subfield 0 is missing
                        if related_name.get('identifier'):
                            self.get_linked_ids(identities, related_name['identifier'], ids, mergeable_relations)
                        else:
                            logging.error('Record %s is missing one of its 510 0 subfields'%record_id)
                if len(ids) > 1:
                    data = {}
                    merged_ids.extend(ids)
                    for identifier in ids:
                        data[identifier] = {'ISNI': identities[identifier]['ISNI'],
                                            'isNot': set(),
                                            'merge': set(),
                                            'delete': False}
                        if identifier in not_requested_ids:
                            data[identifier]['delete'] = True
                    merged_id_clusters.append(data)  
        clustered_ids = {}
        for cluster in merged_id_clusters:
            for cluster_id in cluster:
                for other_id in cluster:
                    if cluster_id != other_id:
                        if cluster[cluster_id]['ISNI'] and cluster[other_id]['ISNI']:
                            if cluster[cluster_id]['ISNI'] != cluster[other_id]['ISNI']:
                                cluster[cluster_id]['isNot'].add(cluster[other_id]['ISNI'])
                            else:
                                if cluster_id not in not_requested_ids and other_id not in not_requested_ids:
                                    logging.error("Local identifiers %s and %s have same ISNI"%(cluster_id, other_id))
                                    not_requested_ids.update([cluster_id, other_id])
                                elif not cluster[cluster_id]['delete']:
                                    cluster[cluster_id]['merge'].add(other_id)
                                    # id added for requesting merged record catalogue:
                                    if not cluster_id in clustered_ids:
                                        clustered_ids[cluster_id] = set()
                                    clustered_ids[cluster_id].add(other_id)
                                    if not other_id in clustered_ids:
                                        clustered_ids[other_id] = set() 
                                    clustered_ids[other_id].add(cluster_id)
                        if cluster[other_id]['ISNI']:
                            if cluster[cluster_id]['ISNI'] != cluster[other_id]['ISNI']:
                                cluster[cluster_id]['isNot'].add(cluster[other_id]['ISNI'])
        resource_ids = set()
        for record_id in requested_ids:
            if args.created_after or args.modified_after:
            #if record_id in current_ids or record_id in related_identities:
                if record_id in current_ids or record_id in linked_ids:
                    resource_ids.add(record_id)
            else:
                resource_ids.add(record_id)
            if record_id in clustered_ids:
                resource_ids.union(clustered_ids[record_id])
        for cluster in merged_id_clusters:
            for cluster_id in cluster:
                if cluster[cluster_id]['delete'] and cluster[cluster_id]['merge']:
                    logging.error("Record %s has related identities with same ISNI, but lacking 983 field"%cluster_id)
                    for cluster_id in cluster:
                        not_requested_ids.add(cluster_id)
            for cluster_id in cluster:
                if cluster_id not in not_requested_ids:
                    identities[cluster_id] = self.merge_identities(cluster_id, cluster[cluster_id]['merge'], identities)
                    identities[cluster_id]['isNot'] = cluster[cluster_id]['isNot']

        for record_id in resource_ids:
            if not args.resource_files:
                if (args.created_after or args.modified_after) and record_id not in current_ids:
                    pass
                elif record_id not in self.resource_list.titles:
                    self.api_search_resources(record_id)
            if record_id in identities:
                identities[record_id]['resource'] = self.sort_resources(record_id, identity['languageOfIdentity'])

        for record_id in identities:
            if not 'isNot' in identities[record_id]:
                identities[record_id]['isNot'] = []
            # delete related names with invalid relation types
            for related_name in identities[record_id]['isRelated']:
                if 'identifier' in related_name:
                    related_id = related_name['identifier']
                    if related_id in identities:
                        if identities[related_id]['ISNI']:
                            related_name['ISNI'] = identities[related_id]['ISNI']
            if identities[record_id]['identityType'] == 'personOrFiction':
                relation_dict_type = "person relation types"
            if identities[record_id]['identityType'] == 'organisation':
                relation_dict_type = "organisation relation types" 
            deletable_relations = []
            for idx, related_name in enumerate(identities[record_id]['isRelated']): 
                relationType = related_name['relationType']
                relationType = self.term_encoder.encode_term(relationType, relation_dict_type)
                if not relationType:
                    if not(identities[record_id]['identityType'] == 'organisation' and \
                        related_name['identityType'] == 'organisation'):
                        if not related_name['relationType']:
                            related_name['relationType'] = "undefined or unknown"
                    elif idx not in deletable_relations:
                        deletable_relations.append(idx)
                else:
                    related_name['relationType'] = relationType            
                # delete merged organisation names from related names 
                if 'identifier' in related_name:
                    if related_name['identifier'] in not_requested_ids and idx not in deletable_relations:
                        deletable_relations.append(idx)

            deletable_relations.reverse()
            for idx in deletable_relations:
                del(identities[record_id]['isRelated'][idx])

        # request related identities in case that identity merged to it is updated:
        merged_ids = set(identifier for cluster in merged_id_clusters for identifier in cluster)
        for identifier in merged_ids:
            if identifier in related_identities and identifier not in not_requested_ids:
                deletable_identities.remove(identifier)

        del_counter = 0
        for record_id in identities:
            if not identities[record_id].get('resource'):
                deletable_identities.add(record_id)
            elif (args.created_after or args.modified_after) and record_id not in current_ids:
                deletable_identities.add(record_id)
            else:
                if not identities[record_id]['resource']:
                    deletable_identities.add(record_id)
                    del_counter += 1
        
        logging.info("Number of discarded records without resources: %s"%del_counter) 
        logging.info("Number of identities to be converted: %s"%(len(identities) - len(deletable_identities)))

        deletable_identities = deletable_identities.union(not_requested_ids)
        for idx in deletable_identities:
            if idx in identities:
                del(identities[idx]) 

        if type(reader) in [MARCReader, aleph_seq_reader.AlephSeqReader]:
            reader.close()

        return identities

    def get_related_identifiers(self, record_id, identities, related_ids):
        """
        Get identifiers of organisations predecessors and successors for ISNI isNot element
        :param record_id: organisatio record's identifier
        :param identities: a dict of identity data gathered by get_authority_data function
        """
        if record_id in identities:
            for related_name in identities[record_id]['isRelated']:
                if related_name['identityType'] == 'organisation':
                    identifier = related_name['identifier']
                    if identifier not in related_ids:
                        related_ids.append(identifier)
                        self.get_related_identifiers(identifier, identities, related_ids)

    def merge_identities(self, identifier, merged_ids, identities):
        """
        merge related identities that are predecessors or successors of an organisation
        merges list of identities discarding duplicate information
        :param identifier: local identifier of an organisation to which 
        :param merged_ids: local identifiers of records to be merged with the record with identifier in above parameter
        :param identities: a dict of identity data 
        """
        merged_identity = identities[identifier]
        related_identities = []
        for identifier in merged_ids:
            merged_identity['organisationNameVariant'].append(identities[identifier]['organisationName'])
            merged_identity['resource'].extend(identities[identifier]['resource'])
        for related_identity in merged_identity['isRelated']:
            if related_identity.get('identifier'):
                if related_identity['identifier'] not in merged_ids:
                    related_identities.append(related_identity)
        del(merged_identity['isRelated'])     
        merged_identity['isRelated'] = related_identities
        if 'usageDateFrom' in merged_identity:
            del(merged_identity['usageDateFrom'])
        if 'usageDateTo' in merged_identity:
            del(merged_identity['usageDateTo'])
        self.sort_resources(identifier, merged_identity['languageOfIdentity'])
        return merged_identity 

    def get_linked_ids(self, identities, related_id, identifiers, mergeable_relations):
        """ 
        Function to that recursively searches all identifiers of related names with relation types defined in parameter mergeable_relations.
        Related identifiers are used for merging identities not distinct enough from each other to have own ISNI identifier
        :param identities: dict of identities 
        :param related_id: identifier of related name 
        :param identifiers: identifiers of related names
        :param mergeable_relations: relation types to look for when searching for identities to be merged 
        """
        identifiers.append(related_id)
        for isRelated in identities[related_id]['isRelated']:
            if 'identifier' in isRelated:
                if isRelated['identityType'] == 'person':
                    if isRelated['relationType'] in mergeable_relations:
                        if isRelated['identifier'] not in identifiers:
                            self.get_linked_ids(identities, isRelated['identifier'], identifiers, mergeable_relations)

    def get_related_names(self, record):
        """ 
        Get related names of persons or organisations in authority data
        :param record: MARC21 record data
        """
        related_names = []
        pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
        
        relation_fields = ['373', '500', '510']
        for rf in relation_fields:
            for field in record.get_fields(rf):
                relationType = None
                # identifies is not an ISNI data element, but local identifier of the related record 
                # converted to ISNI identifier later if possible 
                related_name = {}
                if field.tag == "373" and record['100']:
                    related_name['identityType'] = "organisation"
                    related_name['relationType'] = "isAffiliatedWith"
                    for sf in field.get_subfields("s"):
                        if pattern.fullmatch(sf):
                            related_name['startDateOfRelationship'] = sf
                    for sf in field.get_subfields("t"):
                        if pattern.fullmatch(sf):
                            related_name['endDateOfRelationship'] = sf
                
                    for name in field.get_subfields("a"):
                        related_name['organisationName'] = {"mainName": name}
                        copied_name = copy.copy(related_name)
                        related_names.append(copied_name)
                elif field.tag == "500" or field.tag == "510":
                    if field.tag == "500":
                        related_name['identityType'] = "personOrFiction"
                        related_name['personalName'] = self.get_personal_name(id, field)
                    if field.tag == "510":
                        related_name['identityType'] = "organisation"
                        related_name['organisationName'] = self.get_organisation_name(field, record)
                    if field['0']:
                        related_name['identifier'] = re.sub("[\(].*?[\)]", "", field['0'])
                    for sf in field.get_subfields("i"):
                        relationType = sf.replace(":", "")
                    if record['110']:
                        for sf in field.get_subfields("w"):
                            if sf == "a":
                                relationType = "supersedes"
                            elif sf == "b":
                                relationType = "isSupersededBy"
                            elif sf == "t":
                                relationType = "isUnitOf"
                        if not relationType and field.tag == "500":
                            relationType = "undefined or unknown"
                    if record['100'] and not relationType:
                        relationType = "undefined or unknown"
                    if relationType:
                        related_name['relationType'] = relationType
                        related_names.append(related_name)

        return related_names

    def get_organisation_type(self, record):   
        """
        There can be no multiple organisation types in one ISNI request.
        If more than one, first one is selected.
        Value "Other to be defined" is discarded from list of values, if more than one. 
        :param record: MARC21 record data
        """
        niso_values = []
        organisation_type = None
        for field in record.get_fields("368"):
            for sf in field.get_subfields("a"):
                niso_value = self.term_encoder.encode_term(sf, "organisation types")
                if niso_value not in niso_values:
                    niso_values.append(niso_value)
        if niso_values:
            if len(niso_values) > 1 and "Other to be defined" in niso_values:
                niso_values.remove("Other to be defined")
            organisation_type = niso_values[0]
        else:
            organisation_type = "Other to be defined"     
        return organisation_type

    def get_personal_name(self, id, field):
        """
        Get personal names data from MARC21 fields 100 and 400 for ISNI  
        :param id: identifier from MARC21 field 001
        :param field: MARC21 field with tag 100
        """
        forename = None
        surname = None
        nameUse = None
        nameTitle = None
        numeration = None 
        # Note: 100 c is repeatable, ISNI nameTitle is not
        for sf in field.get_subfields("c"):
            if "fiktiivinen hahmo" in sf:
                nameUse = "fictional character"
            sf = sf.replace("(", "")
            sf = sf.replace(")", "")
            if sf.endswith(","):
                sf = sf[:-1]
            nameTitle = sf
                
        if not nameUse:
            nameUse = "public"

        for sf in field.get_subfields("a"):     
            #  discard variant names ending with hyphen made for library OPAC
            #  Note: some relevant names, e. g. pseudonyms may be discarded too
            if field.tag == "400":
                if sf.endswith("-") or sf.endswith("-,") and len(sf) > 10:
                    return
            if field.indicators[0] == "0":
                surname = sf
                if surname.endswith(","):
                    surname = surname[:-1]
            elif field.indicators[0] == "1":    
                if sf.endswith(","):
                    sf = sf[:-1]
                if not "," in sf:
                    surname = sf
                else:    
                    surname, forename = sf.split(',', 1)
                    surname = surname.replace(",", "")
                    forename = forename.replace(",", "")
                    surname = surname.strip()
                    forename = forename.strip()
            elif field.indicators[0] == "3":
                # name of a family not to be included
                surname = None
            else:
                logging.error("%s: invalid indicator in field: %s"%(id, field))
                return
        for sf in field.get_subfields("b"):
            numeration = sf

        personalName = {
            'nameUse': nameUse,
            'surname': surname,
            'forename': forename,
            'numeration': numeration,
            'nameTitle': nameTitle,            
        }
        if surname:
            return personalName

    def get_fuller_names(self, record):
        """
        Get fuller personal names from record
        :param record: MARC21 record data
        """
        fuller_names = []
        for field in record.get_fields('100'):
            for sf in field.get_subfields("a"):
                surname = sf.split(",")[0]
            for sf in field.get_subfields("q"):
                sf = sf.replace("(", "")
                sf = sf.replace(")", "")
                sf = sf.replace(",", "")
                sf = sf.strip()
                if field.indicators[0] == "0":
                    names = sf.split(" ")
                    names = list(map(str.strip, names))
                    forename = ""
                    surname = names[-1]
                    if len(names) > 1:
                        for name in names[:-1]:
                            forename += name + " "
                        forename = forename.strip()
                    if not forename:
                        forename = None
                    fuller_names.append({"forename": forename, "surname": surname})
                else:
                    fuller_names.append({"forename": sf, "surname": surname})
        for field in record.get_fields('378'):
            for sf in field.get_subfields("q"):
                fuller_names.append({"forename": sf, "surname": surname})
        for field in record.get_fields("680"):
            for sf in field.get_subfields('i'):
                if sf.startswith("Täydellinen nimi:"):
                    sf = sf.replace("Täydellinen nimi:", "")
                    forename = ""
                    surname = ""
                    junior_name = ""
                    if "Jr." in sf:
                        junior_name = " Jr."
                    sf = sf.replace("Jr.", "")
                    names = sf.split(",")
                    names = names[0].strip()
                    names = names.split(" ")
                    names = list(map(str.strip, names))
                    surname = names[-1]
                    if len(names) > 1:
                        for name in names[:-1]:
                            forename += name + " "
                        forename = forename.strip()
                        forename += junior_name    
                    if surname:
                        fuller_names.append({"forename": forename, "surname": surname})
                    else:
                        logging.error("%s: malformed fuller name: %s"%(id, field))
        return fuller_names

    def get_organisation_name(self, field, record):
        """
        Get organisation varnames data from MARC21 fields 110, 410 for ISNI  
        :param field: MARC21 field from which name data is extracted
        :param record: MARC21 record data
        """

        mainName = None
        subdivisionName = []
        for sf in field.get_subfields("a"):
            mainName = sf
            if field. tag == "110":
                mainName = re.sub("[\(].*?[\)]", "", mainName)
                mainName = mainName.strip()
        if mainName:
            for sf in field.get_subfields("b"):
                subdivisionName.append(sf)
        else:
            logging.error("%s: subfield a missing: %s"%(record['001'].data, field))
            return

        return {"mainName": mainName, "subdivisionName": subdivisionName}           

    def get_dates(self, field, identity_type):
        """
        Get Data elemant values document for valid formats YYY-MM-DD preferred 
        ISNI AtomPub schema says that:
        dateType indicates if one of the dates is approximate,
        either "circa" meaning within a few (how many -is it 5?) years or
        "flourished" meaning was active in these years
        :param field: MARC21 field number 046
        :param identity_type: either string 'personOrFiction' or 'organisation'
        """
        date_type = None
        start_date = None
        end_date = None

        date_subfields = ['f', 'g', 'q', 'r', 's', 't']

        for code in date_subfields:
            for d in field.get_subfields(code):
                is_valid = False
                # year in format YYYY, YYYY-MM, YYYY-MM-DD
                pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
                if pattern.fullmatch(d):
                    is_valid = True
                elif len(pattern.findall(d)) == 1 and code in ['f', 'g']:
                    if d.endswith("~"):
                        d = d.replace("~", "")
                        if pattern.fullmatch(d):
                            date_type = "circa"
                            is_valid = True
                # year in edtf format [YYYY,YYYY]
                edtf_pattern = re.compile(r'\[-?\d{4},-?\d{4}\]')
                if edtf_pattern.fullmatch(d):
                    d = d.replace("[", "").replace("]", "")
                    first_year, second_year = d.split(',')
                    if int(second_year) - int(first_year) == 1:
                        d = first_year
                        date_type = "circa"
                        is_valid = True
                if is_valid:
                    if code in ['f', 'q', 's']:
                        if code != 'f' or identity_type != 'organisation':
                            start_date = d
                    if code in ['g', 'r', 't']:
                        if code != 'g' or identity_type != 'organisation':
                            end_date = d
                    if code in ['q', 'r', 's', 't'] and identity_type == 'personOrFiction':
                        date_type = "flourished"
        if identity_type == 'personOrFiction':
            return {"birthDate": start_date, "deathDate": end_date, "dateType": date_type}
        elif identity_type == 'organisation':
            return {"usageDateFrom": start_date, "usageDateTo": end_date}

    def api_search_resources(self, identity_id):
        query_strings = []
        records = []
        # query parameters for Fikka search
        query_strings.append("melinda.asterinameid=" + identity_id)
        query_strings.append(" AND (melinda.authenticationcode=finb OR melinda.authenticationcode=finbd)")
        record_position = 1
        additional_parameters = {'maximumRecords': '100', 'startRecord': str(record_position)}
        logging.info("Requesting bibliographical records for authority record %s"%identity_id)
        response = self.sru_bib_query.api_search(query_strings, additional_parameters)
        response_records = parse_sru_response.get_records(response)    
        if response_records:    
            records.extend(response_records)
        number = parse_sru_response.get_number_of_records(response)
        # if less than 10 query results, query from Melinda
        if number < 10:
            del query_strings[1]
            query_strings.append(" NOT melinda.authenticationcode=finb NOT melinda.authenticationcode=finbd")
            response = self.sru_bib_query.api_search(query_strings, additional_parameters)
            response_records = parse_sru_response.get_records(response) 
            if response_records:         
                records.extend(response_records)       
            number = parse_sru_response.get_number_of_records(response)
        max_number = int(self.config['BIB SRU API'].get('total_records'))
        if number > max_number:
            number = max_number
        record_position += 50
        while (record_position - 1 < number):
            additional_parameters = {'maximumRecords': '100', 'startRecord': str(record_position)}
            response = self.sru_bib_query.api_search(query_strings, additional_parameters)
            if response_records:  
                records.extend(parse_sru_response.get_records(response))
            record_position += 50
        for record in records:
            if record:
                self.resource_list.get_record_data(record, identity_id) 

    def sort_resources(self, identity_id, languages=None):
        """
        :param identity_id: identifier of identity
        :param languages: a list of languages of identity in relevance order
        return a list of titles of work with a maximum item number defined in instance variable
        """
        resources = []
        if identity_id in self.resources:
            resources.extend(self.resources[identity_id])
        if resources:
            mergeable_resources = []
            for idx1 in range(len(resources)):
                # add more relevance to titles with multiple editions and include only the title as metadata
                resources[idx1]['relevance'] = 1
                if idx1 not in mergeable_resources:
                    for idx2 in range(idx1 + 1, len(resources)):
                        if resources[idx1]['title'] == resources[idx2]['title']:
                            mergeable_resources.append(idx2)
                            resources[idx1]['relevance'] += 1
                            resources[idx1]['creationClass'] = None 
                            resources[idx1]['publisher'] = None 
                            resources[idx1]['date'] = None 
                            resources[idx1]['creationRole'] = None       
            mergeable_resources.sort()
            mergeable_resources.reverse()
            for idx in mergeable_resources:
                del(resources[idx])
            
            sorting_order = ['date', 'language', 'relevance', 'role']
            for sorting_key in sorting_order:    
                if sorting_key == 'language':
                    sort_order = {}
                    for idx, l in enumerate(languages):
                        sort_order[l] = idx
                    resources_languages = set()
                    for r in resources:
                        if r['language'] is not None:
                            resources_languages.add(r['language'])
                    resources_languages = sorted(list(resources_languages))
                    for rl in resources_languages:
                        if rl not in sort_order:
                            sort_order[rl] = len(sort_order)
                    sort_order[None] = len(sort_order)
                    resources = sorted(resources, key=lambda val: sort_order[val['language']])
                else:
                    reverse=False
                    if sorting_key == 'relevance':
                        reverse = True
                    resources = sorted(resources, reverse=reverse, key=lambda d:(
                        d[sorting_key]==None,
                        d[sorting_key]))

        return resources[:self.max_number_of_titles]

    def write_isni_fields(self, isnis, args):
        """
        A function to write ISNI identifiers into Aleph Sequential format.
        Write ISNI identifier even if it exists in record in order to
        update record to get cataloguer name to record.
        :param isnis: a dict cotaining local identifiers and assigned ISNIs
        :param args: command line arguments
        """
        with io.open(args.output_marc_fields, 'w', encoding = 'utf-8', newline='\n') as output:
            for record_id in isnis:
                record = self.records[record_id]
                isni_changed = False
                isni_missing = True
                catalogued = False
                fields = []
                isni = isnis[record_id]
                for field in record.get_fields("CAT"):
                    for sf in field.get_subfields('a'):
                        if sf in self.cataloguers:
                            catalogued = True
                for field in record.get_fields("024"):
                    isni_found = False
                    if field['2'] and field['a']:
                        if field['2'] == "isni":
                            isni_found = True
                            isni_missing = False
                            if field['a'] != isni:
                                isni_changed = True
                    if not isni_found:
                        f = self.create_aleph_seq_field(record_id,
                                                        field.tag,
                                                        field.indicators[0],
                                                        field.indicators[1],
                                                        field.subfields)
                        fields.append(f)
                if isni_changed or isni_missing or not catalogued:
                    for f in fields:
                        output.write(f + "\n")
                    output.write(record_id + " 0247  L $$a" + isni + "$$2isni\n")
    
    def create_aleph_seq_field(self, record_id, tag, indicator_1, indicator_2, subfields):
        """
        converts MARC21 field data into Aleph library system's sequential format
        :param record_id: a dict cotaining local identifiers and assigned ISNIs
        :param tag: MARC field tag
        :param indicator_1: first MARC field indicator 
        :param indicator_2: second MARC field indicator 
        :param subfields: a list of subfields with subfield code and content one after another
        """
        seq_field = record_id
        seq_field += " " + tag + indicator_1 + indicator_2
        seq_field += " L "
        for idx in range(0, len(subfields), 2):
            seq_field += "$$"
            seq_field += subfields[idx] + subfields[idx + 1]
        return seq_field