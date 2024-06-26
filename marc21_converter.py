import logging
from datetime import datetime
from resource_list import ResourceList
from term_encoder import TermEncoder
from validators import Validator
from tools import api_query
from tools import parse_sru_response
from tools import parse_oai_response
from pymarc import MARCReader, Field, Subfield
from tools import aleph_seq_reader
import copy
import io
import json
import re
import sys

class MARC21Converter:
    """
        A class to collect data from mrc binary files to ISNI Atom Pub XML format.
    """
    def __init__(self, config):
        self.term_encoder = TermEncoder()
        self.validator = Validator()
        self.config = config
        self.records = {}
        self.resources = None
        self.sru_bib_query = None
        self.request_ids = set()

    def get_linked_organisation_records(self, id, linked_ids, identities):
        """
        Recursively get identifiers of organisations with successors or predecessors to be merged
        :param id: local identifier of organisation record
        :param linked_ids: list of identifiers that may be merged
        :param identities: dict of identity information
        """
        if identities[id]['isni load']:
            for related in identities[id]['isRelated']:
                if related['identityType'] == 'organisation' and related['relationType'] in ['supersedes', 'isSupersededBy']:
                    if 'identifier' in related:
                        related_id = related['identifier']
                        if related_id not in linked_ids:
                            related_org = identities[related_id]
                            if identities[id]['ISNI']:
                                if related_org['ISNI'] == identities[id]['ISNI']:
                                    if related_org['isni load']:
                                        error_message = 'Record ' + related_id + ' has same ISNI, but 983 field missing'
                                        identities[id]['errors'].append(error_message)
                                    else:
                                        linked_ids.append(related_id)
                                        self.get_linked_organisation_records(related_id, linked_ids, identities)
                                else:
                                    if related_org['ISNI']:
                                        identities[id]['isNot'].append({'type': 'ISNI', 'identifier': related_org['ISNI']})

    def request_linked_records(self, record, linked_records, linked_cluster, linked_ids):
        """
        Recursively request all records that are linked to organisation authority record with MARC field 510
        :param record: MARC21 record of an organisation
        :param linked_records: list of ids of recursively requested MARC21 records
        :param linked_ids: list of identifiers of interlinked authority records
        """
        tags = ['500', '510']
        for tag in tags:
            for field in record.get_fields(tag):
                linked_id = None
                if '0' in field and field['0']:
                    linked_id = re.sub("[\(].*?[\)]", "", field['0'])
                    if linked_id not in linked_ids:
                        linked_ids.add(linked_id)
                        linked_cluster.add(linked_id)
                        parameters = {'doc_num': linked_id}
                        response = self.author_query.api_search(parameters=parameters)
                        marc_record = parse_oai_response.get_records(response)[0]
                        if '001' in marc_record:
                            linked_records[marc_record['001'].data] = marc_record
                            self.request_linked_records(marc_record, linked_records, linked_cluster, linked_ids)

    def read_marc_records(self, args):
        """
        Reads MARC21 records to memory from an authority file or from API requests
        :param args: Command line arguments
        """
        marc_records = {}
        if args.authority_files:
            if args.format == "marc21":
                reader = MARCReader(open(args.authority_files, 'rb'), to_unicode=True)
            elif args.format == "alephseq":
                reader = aleph_seq_reader.AlephSeqReader(open(args.authority_files, 'r', encoding="utf-8"))
            else:
                logging.error("Not valid format to convert from: "%args.format)
                sys.exit(2)
            record = ""
            while record is not None:
                try:
                    record = next(reader, None)
                    if record and '001' in record:
                        record_id = record['001'].data
                        marc_records[record_id] = record
                        self.request_ids.add(record_id)
                except Exception as e:
                    logging.exception(e)
            reader.close()
        else:
            logging.info("Requesting authority records with API")
            if self.request_ids:
                section = self.config['AUT X API']
                self.author_query = api_query.APIQuery(config_section=section)
                for id in self.request_ids:
                    parameters = {'doc_num': id}
                    response = self.author_query.api_search(parameters=parameters)
                    record = parse_oai_response.get_records(response)[0]
                    if not id in marc_records:
                        marc_records[id] = record
            elif args.modified_after or args.created_after or args.until:
                section = self.config['AUT OAI-PMH API']
                self.author_query = api_query.APIQuery(config_section=section)
                query_parameters = {'verb': 'ListRecords'}
                if args.modified_after:
                    query_parameters['from'] = args.modified_after
                elif args.created_after:
                    query_parameters['from'] = args.created_after
                if args.until:
                    query_parameters['until'] = args.until
                response = self.author_query.api_search(parameters=query_parameters)
                self.request_ids = parse_oai_response.get_identifiers(response, query_parameters)
                requested_records = parse_oai_response.get_records(response, query_parameters)
                for record in requested_records:
                    if '001' in record:
                        marc_records[record['001'].data] = record
                token = parse_oai_response.get_resumption_token(response, query_parameters)
                while token:
                    query_parameters = {'resumptionToken': token, 'verb': 'ListRecords'}
                    response = self.author_query.api_search(parameters=query_parameters)
                    self.request_ids.update(parse_oai_response.get_identifiers(response, query_parameters))
                    requested_records = parse_oai_response.get_records(response, query_parameters)
                    for record in requested_records:
                        if '001' in record:
                            marc_records[record['001'].data] = record
                    token = parse_oai_response.get_resumption_token(response, query_parameters)
                if not self.request_ids:
                    logging.error("No updated records found within time interval given in parameters")
                    sys.exit(2)
        # get cataloging identifiers whose modification to records are neglected
        self.cataloguers = self.config_values_to_python_object('SETTINGS', 'cataloguers')
        for marc_id in marc_records:
            creation_date = None
            modification_date = None
            record = marc_records[marc_id]
            for field in record.get_fields("CAT"):
                if 'a' in field:
                    cataloguer = field['a']
                    if cataloguer not in self.cataloguers:
                        for sf in field.get_subfields('c'):
                            formatted_date = sf[:4] + "-" + sf[4:6] + "-" + sf[6:8]
                            if not creation_date:
                                creation_date = formatted_date
                            modification_date = formatted_date
            if args.created_after:
                if creation_date < args.created_after or args.until and creation_date >= args.until:
                    self.request_ids.discard(marc_id)
            if args.modified_after:
                if modification_date < args.modified_after or args.until and modification_date >= args.until:
                    self.request_ids.discard(marc_id)
        if not args.authority_files:
            section = self.config['AUT X API']
            self.author_query = api_query.APIQuery(config_section=section)
            linked_ids = set()
            added_records = {}
            for marc_id in marc_records:
                if marc_id in self.request_ids:
                    linked_records = {}
                    linked_cluster = {marc_id}
                    if marc_id not in linked_ids:
                        linked_ids.add(marc_id)
                        self.request_linked_records(marc_records[marc_id], linked_records, linked_cluster, linked_ids)
                    added_records.update(linked_records)
            marc_records.update(added_records)
            if not self.request_ids:
                logging.error("No records found for conversion with command line arguments")
                sys.exit(2)

        return marc_records

    def get_authority_data(self, args, request_ids=set()):
        """
        Converts MARC21 authority bibliographic record data to dict
        :param args: parameters that are passed to converter as command line arguments
        :param request_ids: set of local identifiers of records to be converted into ISNI request
        """
        self.request_ids = request_ids
        if args.resource_files:
            self.resources = ResourceList(args.resource_files, args.format).titles
        else:
            self.resource_list = ResourceList()
            self.resources = self.resource_list.titles
            section = self.config['BIB SRU API']
            self.sru_bib_query = api_query.APIQuery(config_section=section)
        # Identifiers of identities to be removed from ISNI request
        deletable_identities = set()

        self.max_number_of_titles = int(self.config['SETTINGS'].get('max_titles'))
        if args.identity_types == "persons":
            convertible_fields = ['100']
        elif args.identity_types == "organisations":
            convertible_fields = ['110']
        else:
            convertible_fields = ['100', '110']
        isnis = {}
        identities = {}

        records = self.read_marc_records(args)
        for record_id in records:
            record = records[record_id]
            if not record_id or not any(f in record for f in convertible_fields):
                if record_id:
                    self.request_ids.discard(record_id)
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
            for field in record.get_fields('368'):
                for sf in field.get_subfields('a'):
                    if sf in ['festivaali',
                              'hiippakuntakokous',
                              'kilpailu (tapahtuma)',
                              'kirkolliskokous',
                              'kokous',
                              'konferenssi',
                              'konferenssi/seminaari',
                              'näyttely',
                              'pappeinkokous',
                              'piispainkokous',
                              'seminaari',
                              'taidetapahtuma',
                              'tapahtuma',
                              'urheilutapahtuma']:
                        removable = True
            for field in record.get_fields('924'):
                if 'x' in field:
                    self.request_ids.discard(record_id)
            if removable:
                if record_id in self.request_ids:
                    self.request_ids.discard(record_id)
                continue

            identities[record_id] = {}
            identity = identities[record_id]
            identity['isni load'] = True
            self.records[record_id] = record
            for field in record.get_fields('983'):
                for sf in field.get_subfields('a'): 
                    if sf == "ei-isni-loadi-ed":
                        identity['isni load'] = False
                        deletable_identities.add(record_id)
            identity['errors'] = []
            identity['isNot'] = []

            if args.identifier:
                identity['identifier'] = "(" + args.identifier + ")" + record_id
            else:
                identity['identifier'] = record_id
            identity['isRelated'] = self.get_related_names(record, identity)
            if '100' in record:
                identity['identityType'] = 'personOrFiction'
                personal_name = None
                try:
                    personal_name = self.get_personal_name(record['100'])
                    if not personal_name:
                        del(identities[record_id])
                except ValueError as e:
                    identity['errors'].append(str(e) + " 100")
                if personal_name:
                    identity['personalName'] = personal_name
                    identity['personalNameVariant'] = []
                    for field in record.get_fields('400'):
                        variant = None
                        try:
                            variant = self.get_personal_name(field)
                        except ValueError as e:
                            identity['errors'].append(str(e) + " 400")
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
                    try:
                        fuller_names = self.get_fuller_names(record)
                        for fn in fuller_names:
                            # check for duplicate names:
                            if not any(pnv['surname'] == fn['surname'] and pnv['forename'] == fn['forename'] for pnv in identity['personalNameVariant']):
                                identity['personalNameVariant'].append({'nameUse': 'public', 'surname': fn['surname'], 'forename': fn['forename']})
                    except ValueError:
                        identity['errors'].append(str(e))
            elif '110' in record:
                identity['identityType'] = 'organisation'
                organisation_name = self.get_organisation_name(record['110'], record)
                if not organisation_name['mainName']:
                    identity['errors'].append("Subfield a missing from field %s"%(record['110']))
                else:
                    identity['organisationName'] = organisation_name
                    identity['organisationNameVariant'] = []
                    for field in record.get_fields('410'):
                        variant = self.get_organisation_name(field, record)
                        if variant['mainName']:
                            identity['organisationNameVariant'].append(variant)
                        else:
                            identity['errors'].append("Subfield a missing from field %s"%(field))
                    identity['organisationType'] = self.get_organisation_type(record)
            if record_id in identities:
                identifiers = {}
                identity['ISNI'] = None
                identity['otherIdentifierOfIdentity'] = []
                for field in record.get_fields('024'):
                    identifier_type = None
                    identifier = None
                    if 'a' in field and '2' in field:
                        identifier = field['a']
                        if field['2'] in ["viaf", "orcid", "wikidata"]:
                            identifier_type = field['2'].upper()     
                            if field['2'] == "orcid":
                                valid = self.validator.valid_ORCID(field['a'])
                                if valid:
                                    identifier = field['a'].replace('https://orcid.org/', '')
                                else:
                                    identity['errors'].append("ORCID invalid in record")
                            elif field['2'] == "wikidata":               
                                identifier = identifier.replace('https://www.wikidata.org/wiki/', '')
                            if identifier:
                                identifiers[identifier_type] = identifier
                        elif field['2'] == "isni":
                            identity['ISNI'] = identifier
                            isnis[record_id] = identifier

                for identifier_type in identifiers:
                    identity['otherIdentifierOfIdentity'].append({'identifier': identifiers[identifier_type], 'type': identifier_type})

                general_instruction = None
                for field in record.get_fields('924'):
                    for sf in field.get_subfields('q'):
                        if sf in ['merge', 'isNot']:
                            if general_instruction == 'isNot' and sf == 'merge':
                                general_instruction = 'merge'
                            elif sf == 'isNot':
                                general_instruction = 'isNot'
                            elif sf == 'merge':
                                general_instruction = 'merge'
                for field in record.get_fields('924'):
                    if not 'x' in field:
                        identifier = None
                        identifier_type = None
                        field_instruction = None
                        for sf in field.get_subfields('a'):
                            identifier = sf
                        for sf in field.get_subfields('q'):
                            if sf in ['merge', 'isNot']:
                                field_instruction = sf
                        if not field_instruction:
                            if general_instruction == 'isNot':
                                field_instruction = general_instruction
                        for sf in field.get_subfields('2'):
                            identifier_type = sf
                            if identifier_type == 'isni':
                                identifier_type = 'ISNI'
                            elif identifier_type == 'isni-ppn':
                                identifier_type = 'PPN'
                        if identifier and identifier_type:
                            if field_instruction == 'merge':
                                if identity['ISNI']:
                                    if identity['ISNI'] != identifier:
                                        identity['errors'].append("Field 024 ISNI and field 924 merge command are in conflict")
                                if any(identifier['type'] in ['ISNI', 'PPN'] for identifier in identity['otherIdentifierOfIdentity']):
                                    identity['errors'].append("Duplicate merge commands")
                                identity['otherIdentifierOfIdentity'].append({'identifier': identifier, 'type': identifier_type})
                            if field_instruction == 'isNot':
                                identity['isNot'].append({'identifier': identifier, 'type': identifier_type})
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
                            identity['errors'].append('wrong language code in field 377')
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
                    if 'a' in field:
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
                                identity['errors'].append("ENNAKKOTIEDOT in field 670 malformatted")
                    if resource:    
                        resource['creationClass'] = None
                        resource['creationRole'] = 'aut'
                        resource['publisher'] = None
                        if not record_id in self.resources:
                            self.resources[record_id] = []
                        self.resources[record_id].append(resource)

        merge_ids = {}

        for id in identities:
            linked_ids = []
            if id in self.request_ids:
                self.get_linked_organisation_records(id, linked_ids, identities)
                if linked_ids:
                    merge_ids[id] = linked_ids

        for record_id in identities:
            if not args.resource_files:
                resource_ids = set()
                if record_id in self.request_ids and identities[record_id]['isni load']:
                    resource_ids.add(record_id)
                    if record_id in merge_ids:
                        resource_ids.update(merge_ids[record_id])
                for resource_id in resource_ids:
                    self.api_search_resources(resource_id)
                    if resource_id in self.resources:
                        identities[resource_id]['resource'] = self.resources[resource_id]
            elif record_id in self.resources:
                identities[record_id]['resource'] = self.resources[record_id]
        for merge_id in merge_ids:
            if merge_id in self.request_ids:
                identities[merge_id] = self.merge_identities(merge_id, merge_ids[merge_id], identities)

        for id in identities:
            if len(identities[id]['resource']) > self.max_number_of_titles:
                identities[id]['resource'] = self.get_relevant_resources(identities[id]['resource'], identities[id]['languageOfIdentity'])
        for record_id in identities:
            if not 'isNot' in identities[record_id]:
                identities[record_id]['isNot'] = []
            # delete related names with invalid relation types
            for related_name in identities[record_id]['isRelated']:
                if 'identifier' in related_name:
                    related_id = related_name['identifier']
                    if related_id:
                        if related_id in identities:
                            if identities[related_id]['ISNI']:
                                if self.validator.valid_ISNI_checksum(identities[related_id]['ISNI']):
                                    related_name['ISNI'] = identities[related_id]['ISNI']
                                else:
                                    identities[record_id]['errors'].append('Related record id %s has invalid ISNI %s'
                                                                           %(related_id, identities[related_id]['ISNI']))
                        else:
                            identities[record_id]['errors'].append('Related record id %s not in database'%related_id)

            deletable_relations = []
            for idx, related_name in enumerate(identities[record_id]['isRelated']): 
                relationType = related_name['relationType']
                if not relationType:
                    if not(identities[record_id]['identityType'] == 'organisation' and \
                        related_name['identityType'] == 'organisation'):
                        if not related_name['relationType']:
                            related_name['relationType'] = "undefined or unknown"
                    elif idx not in deletable_relations:
                        deletable_relations.append(idx)
                else:
                    related_name['relationType'] = relationType
            deletable_relations.reverse()
            for idx in deletable_relations:
                del(identities[record_id]['isRelated'][idx])

        del_counter = 0
        for record_id in identities:
            if not identities[record_id]['resource']:
                deletable_identities.add(record_id)
                del_counter += 1

        logging.info("Number of discarded records: %s"%del_counter)
        logging.info("Number of identities to be converted: %s"%(len(identities) - del_counter))

        for idx in identities:
            if idx not in self.request_ids:
                deletable_identities.add(idx)
        for idx in deletable_identities:
            if idx in identities:
                del(identities[idx])

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
        for merge_id in merged_ids:
            merged_identity['organisationNameVariant'].append(identities[merge_id]['organisationName'])
            merged_identity['resource'].extend(identities[merge_id]['resource'])
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

    def get_related_names(self, record, identity):
        """ 
        Get related names of persons or organisations in authority data
        :param record: MARC21 record data
        :param identity: a dict of identity data of the MARC21 record
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
                if field.tag == "373" and '100' in record:
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
                        if field.indicators[0] == "3":
                            continue
                        try:
                            related_name['personalName'] = self.get_personal_name(field)
                            related_name['identityType'] = "personOrFiction"
                        except ValueError as e:
                            identity['errors'].append(str(e) + " 500")
                    if field.tag == "510":
                        related_name['identityType'] = "organisation"
                        related_name['organisationName'] = self.get_organisation_name(field, record)
                        if not related_name['organisationName']['mainName']:
                            identity['errors'].append("Subfield a missing from field %s"%(field))
                    if '0' in field:
                        related_name['identifier'] = re.sub("[\(].*?[\)]", "", field['0'])
                    else:
                        identity['errors'].append(field.tag + " field is missing subfield 0")
                    for sf in field.get_subfields("i"):
                        relationType = sf.replace(":", "")
                        relationType = self.term_encoder.encode_term(relationType, 'relation types')
                        if not relationType:
                            identity['errors'].append("Relation type translation missing for subfield i: %s"%(sf))
                    if '110' in record:
                        for sf in field.get_subfields("w"):
                            if sf == "a":
                                relationType = "supersedes"
                            elif sf == "b":
                                relationType = "isSupersededBy"
                            elif sf == "t":
                                relationType = "isUnitOf"
                        if not relationType and field.tag == "500":
                            relationType = "undefined or unknown"
                    if '100' in record and not relationType:
                        relationType = "undefined or unknown"
                    if relationType and related_name['identityType']:
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

    def get_personal_name(self, field):
        """
        Get personal names data from MARC21 fields 100 and 400 for ISNI  
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
                raise ValueError ("invalid indicator in field")
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
                        raise ValueError("malformed fuller name in field 680")
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
        records = []
        # query parameters for Fikka search
        query = "melinda.asterinameid=" + identity_id + " AND (melinda.authenticationcode=finb OR melinda.authenticationcode=finbd)"
        record_position = 1
        config_parameters = self.config_values_to_python_object('BIB SRU API', 'parameters')
        offset = int(config_parameters['maximumRecords'])
        additional_parameters = {'startRecord': str(record_position)}
        logging.info("Requesting bibliographical records for authority record %s"%identity_id)
        response = self.sru_bib_query.api_search(query=query, parameters=additional_parameters)
        response_records = parse_sru_response.get_records(response)    
        if response_records:    
            records.extend(response_records)
        number = parse_sru_response.get_number_of_records(response)
        # if less than 10 query results, query from Melinda
        if number < 10:
            query = "melinda.asterinameid=" + identity_id + " NOT melinda.authenticationcode=finb NOT melinda.authenticationcode=finbd"
            response = self.sru_bib_query.api_search(query=query, parameters=additional_parameters)
            response_records = parse_sru_response.get_records(response) 
            if response_records:         
                records.extend(response_records)
            number = parse_sru_response.get_number_of_records(response)
        max_number = int(self.config['BIB SRU API'].get('total_records'))
        if number > max_number:
            number = max_number
        record_position += offset
        while (record_position - 1 < number):
            additional_parameters['startRecord'] = str(record_position)
            response = self.sru_bib_query.api_search(query=query, parameters=additional_parameters)
            if response_records:  
                records.extend(parse_sru_response.get_records(response))
            record_position += offset
        for record in records:
            if record:
                self.resource_list.add_record_data(record, identity_id)

    def get_relevant_resources(self, resources, languages=None):
        """
        :param resources: list of resources, containing titles of works and other information
        :param languages: a list of languages of identity in relevance order
        return a list of titles of work with a maximum item number defined in instance variable
        """
        mergeable_resources = []
        for idx1 in range(len(resources)):
            # add more relevance to titles with multiple editions and include only the title as metadata
            resources[idx1]['relevance'] = 1
            if idx1 not in mergeable_resources:
                for idx2 in range(idx1 + 1, len(resources)):
                    if resources[idx1]['title'] == resources[idx2]['title']:
                        mergeable_resources.append(idx2)
                        resources[idx1]['relevance'] += 1
                        #resources[idx1]['creationClass'] = None
                        #resources[idx1]['publisher'] = None
                        #resources[idx1]['date'] = None
                        #resources[idx1]['creationRole'] = None
                        for identifier_type in resources[idx2]['identifiers']:
                            if identifier_type not in resources[idx1]['identifiers']:
                                resources[idx1]['identifiers'][identifier_type] = []
                            resources[idx1]['identifiers'][identifier_type].extend(resources[idx2]['identifiers'][identifier_type])
        mergeable_resources.sort()
        mergeable_resources.reverse()
        # two possible roles author and contributor, sorting and relevance same as alphabetical order
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

    def create_isni_fields(self, isni_response, identifier=""):
        """
        A function to write ISNI identifiers into Aleph Sequential format.
        Write ISNI identifier even if it exists in record in order to
        update record to get cataloguer name to record.
        :param isni_response: a dict containing assigned ISNIs and possible matches
        :param identifier: source code used in ISNI request, e.g. NLFIN
        """
        records = []
        for record_id in isni_response:
            record = self.records[record_id]
            isni_changed = False
            isni_found = False
            update_fields = False
            removable_fields = []
            result = isni_response[record_id]
            date_today = str(datetime.today().date())
            possible_matches = []
            isni = None
            status_field = None
            if result.get('errors'):
                update_fields = True
                record.add_ordered_field(Field(tag = '924', indicators = [' ',' '], subfields=[Subfield(code='x', value='KESKEN-ISNI')]))
                for error in result['errors']:
                    record.add_ordered_field(Field(tag = '924', indicators = [' ',' '], subfields=[Subfield(code='q', value=error)]))
            elif result.get('possible matches'):
                # if record is rich, it is entered to ISNI production with PPN or ISNI, so no actions needed
                if 'ppn' in result or 'isni' in result:
                    pass
                # if record is sparse, it is not entered to ISNI production and possible matches are entered to local record instead
                else:
                    update_fields = True
                    status_field = Field(tag = '924', indicators = [' ',' '], subfields=[Subfield(code='x', value='KESKEN-ISNI')])
                    for id in result['possible matches']:
                        possible_matches.append(id)
                    for field in record.get_fields("924"):
                        if 'a' in field and field['a'] in result['possible matches']:
                            field.add_subfield('d', date_today)
                            local_identifier = record_id
                            if identifier:
                                local_identifier = '(' + identifier + ')'
                            text = "Asteri-id " + local_identifier + " " + record_id + " lisättävä ISNIin tai tarkistettava että merge-merkintä oikein"
                            status_field.add_subfield('q', text)
                            possible_matches.remove(field['a'])
                    for pm in possible_matches:
                        subfields = []
                        if 'isni' in pm:
                            subfields.append(Subfield(code='a', value=pm['isni']))
                            subfields.append(Subfield(code='2', value='isni'))
                        elif 'ppn' in pm:
                            subfields.append(Subfield(code='a', value=pm['ppn']))
                            subfields.append(Subfield(code='2', value='isni-ppn'))
                        if any(record_id == pm['sources'][code] for code in pm['sources']):
                            if len(result['possible matches']) == 1:
                                status_field.add_subfield('q', 'sparse record')
                                continue
                            else:
                                subfields.append(Subfield(code='q', value='=tämä tietue'))
                        elif identifier:
                            local_identifiers = []
                            for code in pm['sources']:
                                if identifier == code:
                                    local_identifiers.append(pm['sources'][code])
                            if local_identifiers:
                                subfields.append(Subfield(code='q', value='=Asterin tietue(et): ' + ', '.join(local_identifiers)))
                        subfields.append(Subfield(code='d', value=date_today))
                        field = Field(tag = '924', indicators = ['7',' '], subfields=subfields)
                        record.add_ordered_field(field)
            elif result.get('reason'):
                update_fields = True
                status_field = Field(tag = '924', indicators = [' ',' '], subfields=[Subfield(code='x', value='KESKEN-ISNI')])
                if result['reason'] == 'no match initial database':
                    status_field.add_subfield('q', 'sparse record')
                else:
                    status_field.add_subfield('q', result['reason'])
            elif result.get('isni'):
                isni = result['isni']
                for field in record.get_fields("924"):
                    update_fields = True
                    removable = True
                    if 'a' in field:
                        if self.validator.valid_ISNI_checksum(field['a']) and isni != field['a']:
                            removable = False
                            while 'd' in field:
                                field.delete_subfield('d')
                            while 'q' in field:
                                field.delete_subfield('q')
                            field.add_subfield('q', 'isNot')
                    if removable:
                        removable_fields.append(field)

                deprecated_isnis = {}
                if 'deprecated isnis' in result:
                    for deprecated_isni in result['deprecated isnis']:
                        deprecated_isnis[deprecated_isni] = False
                for field in record.get_fields("024"):
                    if '2' in field and 'a' in field:
                        if field['2'] == "isni":
                            if field['a'] != isni:
                                if field['a'].replace(' ', '') == isni:
                                    field['a'] = field['a'].replace(' ', '')
                                    isni_found = True
                                else:
                                    removable_fields.append(field)
                                    isni_changed = True
                                update_fields = True
                            else:
                                isni_found = True
                    if '2' in field and 'z' in field:
                        if field['2'] == "isni":
                            if field['z'] not in deprecated_isnis or deprecated_isnis[field['z']]:
                                removable_fields.append(field)
                                update_fields = True
                            deprecated_isnis[field['z']] = True
                if isni_changed or not isni_found:
                    subfields = [Subfield(code='a', value=isni), Subfield(code='2', value='isni')]
                    field = Field(tag = '024', indicators = ['7',' '], subfields=subfields)
                    record.add_ordered_field(field)
                    update_fields = True
                for deprecated_isni in deprecated_isnis:
                    if not deprecated_isnis[deprecated_isni]:
                        subfields = [Subfield(code='z', value=deprecated_isni), Subfield(code='2', value='isni')]
                        record.add_ordered_field(Field(tag = '024', indicators = ['7',' '], subfields=subfields))
                        update_fields = True
            if update_fields:
                if status_field:
                    fields = record.get_fields('924')
                    record.remove_fields('924')
                    record.add_ordered_field(status_field)
                    for field in fields:
                        record.add_ordered_field(field)
                for field in removable_fields:
                    record.remove_field(field)
                # unnecessary 003 fields from OAI-PMH requests
                record.remove_fields('003')
                records.append(record)
    
        return records

    def write_isni_fields(self, file_path, records):
        with io.open(file_path, 'w', encoding = 'utf-8', newline='\n') as fh:
            for record in records:
                field = Field(tag="001", data=str(record.leader))
                fh.write(self.create_aleph_seq_field(record['001'].data, field, leader=True) + "\n")
                for field in record.get_fields():
                    fh.write(self.create_aleph_seq_field(record['001'].data, field) + "\n")

    def create_aleph_seq_field(self, record_id, field, leader=False):
        """
        converts MARC21 field data into Aleph library system's sequential format
        :param record_id: a dict cotaining local identifiers and assigned ISNIs
        :param field: Field object of pymarc library
        :param leader: Boolean value that determines whether field is leader (to preserve special chars of Aleph sequential format)
        """
        seq_field = record_id
        if leader:
            field.tag = "LDR"
        seq_field += " " + field.tag
        if hasattr(field, "data") or leader:
            seq_field += "   L " + field.data
        else:
            seq_field += field.indicators[0] + field.indicators[1] + " L "
            for sf in field.subfields:
                seq_field += "$$"
                subfield = sf.value
                if field.tag in ['100', '110', '111', '400', '410', '411', '500', '510', '511']:
                    subfield = subfield.replace('FI-ASTERI-N', 'FIN11')
                seq_field += sf.code + subfield

        return seq_field

    def config_values_to_python_object(self, section_name, key):
        """
        Converts config file values to Python objects
        :param section_name: name of config file section
        :param key: key in config section above
        """
        try:
            section = self.config[section_name]
            value = section.get(key)
            value = json.loads(value)
            return value
        except json.decoder.JSONDecodeError as e:
            logging.error("Parameters %s malformatted in config.ini section"%(key, section))
            logging.error(e)
            sys.exit(2)