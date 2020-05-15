import logging
from term_encoder import TermEncoder
from validators import Validator
import re
import sys

class MARC21DataCollector:
    """
        A class to collect data from mrc binary files to ISNI Atom Pub XML format.
    """
    def __init__(self):
        self.term_encoder = TermEncoder()
        self.validator = Validator()
       
    def get_author_data(self, args, resources):
        """
        :param args: parameters that are passed to converter as command line arguments
        :param resources: resources parsed by resource_list to a dict object
        """ 
        self.resources = resources
        self.max_number_of_titles = args.max_number
        if args.identity_types == "persons":
            convertible_fields = ['100']
        elif args.identity_types == "organisations":
            convertible_fields = ['110']
        elif args.identity_types == "all":
            convertible_fields = ['100', '110']

        identities = {}
        if args.format == "marc21":
            from pymarc import MARCReader                         
            reader = MARCReader(open(args.authority_files, 'rb'), to_unicode=True)
        elif args.format == "alephseq":
            from tools import aleph_seq_reader                       
            reader = aleph_seq_reader.AlephSeqReader(open(args.authority_files, 'r', encoding="utf-8"))
        else:
            logging.error("Not valid format to convert from: "%args.format)
            sys.exit(2)
        record_id = None
        record = ""
        counter = 0
        while record is not None:
            try:
                record = next(reader, None)     
            except Exception as e:
                logging.exception(e) 
            if record:
                if record['001']:
                    record_id = record['001'].data
                else:
                    continue
            else:
                continue
            if not any(record[f] for f in convertible_fields):
                continue
            
            counter += 1

            identities[record_id] = {}
            identity = identities[record_id]
            # TODO: is it necessary to add parentheses to identifiers?  
            identity['identifier'] = "(" + args.identifier + ")" + record_id
            identity['isRelated'] = self.get_related_names(record)
            if record['100']:
                identity['identityType'] = 'personOrFiction'
                personal_name = self.get_personal_name(record_id, record['100'])      

                # TODO: remove this temp section used for matching bibliographical records after batch load:
                subfield_codes = ['a', 'b', 'c', 'd', 'q']
                name = ""
                for field in record.get_fields('100'):
                    for sc in subfield_codes:
                        for sf in field.get_subfields(sc):
                            if sf.endswith(","):
                                sf = sf[:-1]
                            name += sf
                if name.endswith("."):
                    name = name[:-1]
                identity['established names'] = [name]

                if not personal_name:
                    del(identities[record_id])
                else:
                    identity['personalName'] = personal_name
                    identity['personalNameVariant'] = []
                    # TODO: check for duplicate name variants: 
                    for field in record.get_fields('400'):
                        variant = self.get_personal_name(record_id, field)
                        is_real_name = False
                        for sf in field.get_subfields('4'):
                            if sf == "toni":
                                is_real_name = True
                        if is_real_name:
                            if variant:
                                identity['isRelated'].append({
                                    "identifier": None,
                                    "identityType": 'personOrFiction',
                                    "relationType": 'real name',
                                    "organisationName": None,
                                    "personalName": variant,
                                    "startDateOfRelationship": None,
                                    "endDateOfRelationship": None
                                })
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
                organisation_name = self.get_organisation_name(record_id, record['110'])
                if not organisation_name:
                    del(identities[record_id])
                else:
                    identity['organisationName'] = organisation_name
                    identity['organisationNameVariant'] = []
                    for field in record.get_fields('410'):
                        variant = self.get_organisation_name(record_id, field)
                        if variant:
                            identity['organisationNameVariant'].append(variant)
                    identity['organisationType'] = self.get_organisation_type(record)   
            if record_id in identities:
                identifiers = {}
                identity['otherIdentifierOfIdentity'] = []
                identity['ISNI'] = None
                for field in record.get_fields('024'):
                    identifier_type = None
                    identifier = None
                    for sf in field.get_subfields('2'):
                        identifier_type = sf.upper()                    
                    for sf in field.get_subfields('a'):
                        # TODO: check if ISNI accepts identifiers only in upper case
                        identifier = sf
                    if identifier_type and identifier:
                        if identifier_type == "isni":
                            identity['ISNI'] = identifier
                        identifiers.update({identifier_type: identifier})
                for identifier_type in identifiers:
                    identity['otherIdentifierOfIdentity'].append({'identifier': identifiers[identifier_type], 'type': identifier_type})
                
                for field in record.get_fields('046'):
                    # NOTE: it is assumed that only one MARC21 field for dates is used
                    dates = self.get_dates(field)
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
                for field in record.get_fields("043"):
                    # TODO: check if field n:o 043 and field n:o 370 in conflict or one of them missing?
                    # TODO: organisation's HQ's location to AtomPub element organisation/location/countryCode? 
                    for sf in field.get_subfields("c"):
                        if self.validator.valid_country_code(sf):
                            country_codes.append(sf)
                identity['countriesAssociated'] = country_codes

                uris = []
                for field in record.get_fields("670"):   
                    for sf in field.get_subfields("u"):
                        uris.append(sf)
                identity['URI'] = uris
                #identity['resource'] = self.sort_resources(record_id, identity['languageOfIdentity'])  
                # TODO: use line above instead of line below after batch load
                resources = self.sort_resources(record_id, identity['established names'], identity['languageOfIdentity'])
                if resources:
                    identity['resource'] = resources           
                else:
                    identity['resource'] = []

        # TODO: this is temporary section for merging related records in batch load
        merged_ids = []
        merged_id_clusters = []
        mergeable_relations = ["Myöhempi nimi", "Vaihtunut sukunimi", "Aiempi nimi", "Syntymänimi", "Uskonnollinen nimi", "Maallikkonimi"]
        for record_id in identities:
            if record_id not in merged_ids:
                ids = [record_id]
                for idx, related_name in enumerate(identities[record_id]['isRelated']):
                    relationType = related_name['relationType']
                    # merge identities with only changed names:
                    if relationType in mergeable_relations:
                        self.get_linked_ids(identities, related_name['identifier'], ids, mergeable_relations)
                if len(ids) > 1:
                    merged_ids.extend(ids)
                    merged_id_clusters.append(ids)
        merge_counter = 0
        for cluster in merged_id_clusters:
            mergeable_identities = []
            if len(cluster) > 1:
                # choose the most recent record in database as a record to which other identities are merged:
                merged_id = cluster[-1]
                for cluster_id in cluster:
                    try:
                        mergeable_identities.append(identities[cluster_id])
                        if cluster_id != merged_id:
                            del(identities[cluster_id])
                            merge_counter += 1
                    except KeyError:
                        logging.error("Merging problem with records %s and %s"%(merged_id, cluster_id))
                self.merge_identities(merged_id, mergeable_identities, mergeable_relations) 
        
        merged_ids = set(identifier for cluster in merged_id_clusters for identifier in cluster)

        for record_id in identities:
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
                        related_name['relationType'] = "undefined or unknown"
                    else:
                        deletable_relations.append(idx)
                else:
                    related_name['relationType'] = relationType
                
                # temporary code to delete relations from merged identity to other identities 
                # that were merged. It is possible still to have relation between pseudonym
                # and identity that was deleted in merge 
                if 'identifier' in related_name:
                    if related_name['identifier'] in merged_ids and record_id in merged_ids:
                        deletable_relations.append(idx)
                
            deletable_relations.reverse()
            for idx in deletable_relations:
                del(identities[record_id]['isRelated'][idx])
        
        del_counter = 0
        deletable_identities = set()
        for record_id in identities:
            if not 'resource' in identities[record_id]:
                deletable_identities.add(record_id)
            else:
                if not identities[record_id]['resource']:
                    deletable_identities.add(record_id)
                    del_counter += 1
        logging.info("Number of discarded records without resources: %s"%del_counter) 
        logging.warning("Number of identities to be converted: %s"%len(identities))
        for idx in deletable_identities:
            del(identities[idx]) 

        return identities

    def merge_identities(self, merged_id, identities, mergeable_relations):
        # merge related identities that are not pseudonyms 
        # only temporary function to delete unnecessary records in the batch load
        # merges list of identities discarding duplicate information
        # return last identity in original list given in parameter
        merged_identity = identities[-1]
        for identity in identities[:-1]:
            for key in identity:
                if identity[key]:
                    if key == "isRelated":
                        for relation in identity[key]:
                            if relation['relationType'] not in mergeable_relations:
                                if not any(relation == r for r in merged_identity['isRelated']):
                                    merged_identity['isRelated'].append(relation)
                    elif key == "personalName":
                        if not any(identity[key] == pnv for pnv in merged_identity['personalNameVariant']):
                            merged_identity['personalNameVariant'].append(identity[key])
                    elif key == "personalNameVariant":
                        for variant_name in identity[key]:
                            if not any(variant_name == pnv for pnv in merged_identity['personalNameVariant']):
                                merged_identity['personalNameVariant'].append(variant_name)
                    elif key != "identifier" and type(identity[key]) == list:
                        for prop in identity[key]:
                            if not any(prop == p for p in merged_identity[key]):
                                merged_identity[key].append(prop)     

        # TODO: remove 'established names' parameter afetr batch load   
        self.sort_resources(merged_id, identity['established names'], merged_identity['languageOfIdentity'])
        return merged_identity 

    def get_linked_ids(self, identities, related_id, ids, mergeable_relations):
        # get linked records 
        ids.append(related_id)
        for isRelated in identities[related_id]['isRelated']:
            if 'identifier' in isRelated:
                if isRelated['identityType'] == 'person':
                    if isRelated['relationType'] in mergeable_relations:
                        if isRelated['identifier'] not in ids:
                            self.get_linked_ids(identities, isRelated['identifier'], ids, mergeable_relations)

    def get_related_names(self, record):
        # ISNI/PPN identifiers have to be added later, when all records are read
        related_names = []
        pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
        
        relation_fields = ['373', '500', '510']
        for rf in relation_fields:
            for field in record.get_fields(rf):
                identityType = None
                relationType = None
                organisationName = None
                personalName = None
                startDateOfRelationship = None
                endDateOfRelationship = None
                identifier = None # not an ISNI data element, id of the related record in database 

                # TODO: check for identical names
                if field.tag == "373" and record['100']:
                    # there can be multiple organisation in one 373 field:
                    identityType = "organisation"
                    relationType = "isAffiliatedWith"
                    for sf in field.get_subfields("s"):
                        if pattern.fullmatch(sf):
                            startDateOfRelationship = sf
                    for sf in field.get_subfields("t"):
                        if pattern.fullmatch(sf):
                            endDateOfRelationship = sf
                
                    for name in field.get_subfields("a"):
                        organisationName = {"mainName": name}

                        #  TODO: remove parentheses
                        related_names.append({
                            "identityType": identityType,
                            "relationType": relationType,
                            "organisationName": organisationName,
                            "personalName": personalName,
                            "startDateOfRelationship": startDateOfRelationship,
                            "endDateOfRelationship": endDateOfRelationship
                        })
            
                elif field.tag == "500" or field.tag == "510":
                    if field.tag == "500":
                        identityType = "personOrFiction"
                        personalName = self.get_personal_name(id, field)
                    if field.tag == "510":
                        identityType = "organisation"
                        organisationName = self.get_organisation_name(id, field)
                    if field['0']:
                        identifier = re.sub("[\(].*?[\)]", "", field['0'])
                    for sf in field.get_subfields("i"):
                        relationType = sf
                        relationType = relationType.replace(":", "")
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
                    # TODO: translate relation types here
                    if relationType and (personalName or organisationName):
                        related_names.append({
                            "identifier": identifier,
                            "identityType": identityType,
                            "relationType": relationType,
                            "organisationName": organisationName,
                            "personalName": personalName,
                            "startDateOfRelationship": startDateOfRelationship,
                            "endDateOfRelationship": endDateOfRelationship
                        })
        return related_names

    def get_organisation_type(self, record):   
        """
        There can be no multiple organisation types in one ISNI request.
        If more than one, first one is selected.
        Calue "Other to be defined" is discarded from list of values, if more than one. 
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
        # TODO: remove parenthes from names?
        forename = None
        surname = None
        nameUse = None
        nameTitle = None
        numeration = None 
        # 100 c is repeatable, ISNI nameTitle is not
        #  choose first c subfield or add up?
        for sf in field.get_subfields("c"):
            if "fiktiivinen hahmo" in sf:
                nameUse = "fictional character"
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
                if not "," in sf:
                    logging.warning("%s: wrong indicator in field: %s"%(id, field))
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
                    fuller_names.append({"forename": forename, "surname": surname})
        return fuller_names

    def get_organisation_name(self, id, field):
        # TODO: remove parenthes from names?
        mainName = None
        subdivisionName = []
        
        for sf in field.get_subfields("a"):
            mainName = sf
        if mainName:
            for sf in field.get_subfields("b"):
                subdivisionName.append(sf)
        else:
            logging.error("%s: subfield a missing: %s"%(id, field))
            return
        
        return {"mainName": mainName, "subdivisionName": subdivisionName}           

    def get_dates(self, field):
        """
        Get Data elemant values document for valid formats YYY-MM-DD preferred 
        ISNI AtomPub schema says that:
        dateType indicates if one of the dates is approximate,
        either "circa" meaning within a few (how many -is it 5?) years or
        "flourished" meaning was active in these years
        :param field: MARC21 field number 046 
        """
        dateType = None
        birthDate = None
        deathDate = None
        usageDateFrom = None
        usageDateTo = None

        date_subfields = ['f', 'g', 'q', 'r', 's', 't']

        for df in date_subfields:
            for d in field.get_subfields(df):
                # TODO: '"circa" meaning within a few (how many -is it 5?) years"'      
                is_valid = False
                # year in format YYYY, YYYY-MM, YYYY-MM-DD"
                pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
                if pattern.fullmatch(d):
                    is_valid = True
                if df == "f" or df == "g":
                    if not is_valid and len(pattern.findall(d)) == 1:
                        if d.endswith("~"):
                            d = d.replace("~", "")
                            if pattern.fullmatch(d):
                                dateType = "circa"
                                is_valid = True         
                if is_valid:
                    if df == "f":                         
                        birthDate = d
                    if df == "g":                         
                        deathDate = d
                    #  046 s and t subfields are used both for organisations and persons
                    #  unnecessary data returned here is to be discarded later 
                    if df in ['q', 'r', 's', 't']:
                        dateType = "flourished"
                    if df == "q":                         
                        usageDateFrom = d
                    if df == "r":                         
                        usageDateTo = d
                    if df == "s":                         
                        birthDate = d
                        usageDateFrom = d
                    if df == "t":                         
                        deathDate = d
                        usageDateTo = d
                    
        return {"birthDate": birthDate, 
                "deathDate": deathDate, 
                "dateType": dateType,
                "usageDateFrom": usageDateFrom,
                "usageDateTo": usageDateTo}

    def sort_resources(self, identity_id, established_names, languages=None):
        """
        :param identity_id: identifier of identity
        :param languages: a list of languages of identity in relevance order
        return a list of titles of work with a maximum item number defined in instance variable
        """
        # TODO: remove established_names parameter after batch load
        resources = []
        if identity_id in self.resources:
            resources.extend(self.resources[identity_id])
        for name in established_names:
            if name in self.resources:
                resources.extend(self.resources[name])
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