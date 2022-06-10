import logging
from datetime import datetime
import csv
import configparser
import re

class GramexConverter:
    """
        A class to collect data from Gramex CSV files for ISNI Atom Pub XML format.
    """
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
    
    def convert_time_string(self, time_string):
        values = time_string.split('.')
        return values[2] + "-" + values[1].zfill(2) + "-" + values[0].zfill(2)

    def validate_date(self, date_string):
        pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
        if pattern.fullmatch(date_string):
            return True
        return False

    def names_match(self, name1, name2):
        if name1['surname'] == name2['surname']:
            return True
        if name1['forename'] and name2['forename']:
            if any(forename in name2['forename'] for forename in name1['forename'].split(' ')):
                return True
        return False

    def get_authority_data(self, args, requested_ids=set()):
        """
        :param args: parameters that are passed to converter as command line arguments
        :param requested_ids: a set of local identifiers to be converted into ISNI request
        """ 

        logging.info("Opening %s"%args.authority_files)
        
        """ TODO: remove this temporary code:
        ipda_ids = dict()
        with open('kaikki_IPN_ISNI_Gramex-localID.txt', 'r', encoding='utf-8') as fh:
            for line in fh:
                data = line.split(';')
                if len(data) > 2:
                    ipda_ids[data[2].rstrip()] = {'ISNI': data[1], 'IPN': data[0]} """

        with open(args.authority_files, 'r', encoding='utf-8-sig', newline='') as fh:
            csvreader = csv.reader(fh, delimiter=';', quotechar='"')
       
            keys = ['ID',
                    'Alternative name ID',
                    'LAST_NAME',
                    'FIRST_NAME',
                    'DATE_OF_BIRTH',
                    'DATE_OF_DEATH',
                    'NAME',
                    'CODE']
        
            header = next(csvreader)
            author_indices = {value:n 
                              for n, value in enumerate(header)
                              if value in keys}

            names = dict()
            id_dict = dict()
            for row in csvreader:
                row = [i or None for i in row]
                indice = author_indices.get('CODE')
                name_type = row[author_indices['CODE']] if indice else None
                pseud_id = None
                identifiers = []
                real_id = row[author_indices['ID']]
                if not real_id:
                    continue
                
                if row[author_indices['DATE_OF_BIRTH']]:
                    if row[author_indices['DATE_OF_BIRTH']][:7] > "2004-05":
                        logging.error("Skipping invalid date %s in record %s"%(row[author_indices['DATE_OF_BIRTH']], real_id))
                        continue
                if name_type == 'PS':
                    pseud_id = row[author_indices['Alternative name ID']]
                    identifiers.append(pseud_id)
                    id_dict[pseud_id] = real_id
                identifiers.append(real_id)
                for id in identifiers:
                    if id not in names:
                        names[id] = dict()
                        for key in author_indices:
                            names[id][key] = row[author_indices[key]]
                        names[id]['variant names'] = []
                        names[id]['artist names'] = []
                        names[id]['pseudonyms'] = []
                        names[id]['groups'] = []
                if pseud_id:
                    names[pseud_id]['type'] = 'pseud'
                names[id]['type'] = 'real name'
                indice = author_indices.get('NAME')
                variant_name = row[author_indices['NAME']] if indice else None
                if variant_name:
                    variant_forenames = None
                    variant_surname = None
                    if " " in variant_name:
                        variant_forenames = variant_name[variant_name.find(" ") + 1:]
                        variant_surname = variant_name[:variant_name.find(" ")]
                    else:
                        variant_surname = variant_name
                    whole_name = {'forename': variant_forenames, 'surname': variant_surname}
                    if name_type == "AR":
                        names[id]['variant names'].append({'forename': names[id]['FIRST_NAME'], 'surname': names[id]['LAST_NAME']})
                        names[id]['artist names'].append(whole_name)
                        names[id]['FIRST_NAME'] = variant_forenames
                        names[id]['LAST_NAME'] = variant_surname
                    elif name_type == "PS":
                        names[real_id]['pseudonyms'].append(whole_name)
                        names[pseud_id]['pseudonyms'].append(whole_name)
                    elif name_type == "GR":
                        names[id]['groups'].append(variant_name)
                    elif name_type != "NO":
                        names[id]['variant names'].append(whole_name)

        logging.info("Opening %s"%args.resource_files)
        with open(args.resource_files, 'r', encoding='utf-8-sig', newline='') as fh:
            csvreader = csv.reader(fh, delimiter=';', quotechar='"')
            keys = ['PARTY_ID',
                    'ISRC',
                    'TRACK_TITLE',
                    'MAINARTIST_NAME',
                    'RECORDING_YEAR',
                    'CATALOG_NUMBERS']
            
            header = next(csvreader)
            track_indices = {value:n 
                            for n, value in enumerate(header)
                            if value in keys}

            tracks = dict()
            for row in csvreader:
                row = [i or None for i in row]
                id = row[track_indices['PARTY_ID']]
                if id not in tracks:
                    tracks[id] = []
                track = dict()
                for key in track_indices:
                    value = row[track_indices[key]]
                    track[key] = value
                    if key == 'CATALOG_NUMBERS':
                        number = value.count(',')
                        track['AMOUNT'] = number
                tracks[id].append(track)

        for id in tracks:
            tracks[id] = sorted(tracks[id], key=lambda d:(
                d['ISRC']==None,
                -d['AMOUNT'],
                d['RECORDING_YEAR']
                ))
            max_number_of_titles = int(self.config['SETTINGS'].get('max_titles'))

        identities = dict()
        deletable_identities = set()

        for id in names:
            
            if len(names[id]['pseudonyms']) > 0 and len(names[id]['artist names']) == 0 and names[id]['type'] == 'real name':
                logging.warning("Person not using real name as an artist name, skipping record id %s"%id)
                deletable_identities.add(id)
            identities[id] = {'identifier': id}
            identities[id]['identityType'] = 'personOrFiction'
            identities[id]['isRelated'] = []
            identities[id]['personalName'] = {'nameUse': 'public'}
            if names[id].get('type') == 'pseud':
                real_id = names[id]['ID'] 
                personal_name = names[id]['pseudonyms'][0]
                identities[id]['personalName'].update(personal_name)
                is_related = {'identityType': 'personOrFiction', 'relationType': 'real name'}
                is_related['personalName'] = {'nameUse': 'public',
                                              'forename': names[real_id]['FIRST_NAME'], 
                                              'surname': names[real_id]['LAST_NAME']}
                identities[id]['isRelated'].append(is_related)
            else:
                identities[id]['personalName'].update({'forename': names[id]['FIRST_NAME'],
                                                       'surname': names[id]['LAST_NAME']})
                for pseudonym in names[id]['pseudonyms']:
                    is_related = {'identityType': 'personOrFiction', 'relationType': 'pseud'}
                    is_related['personalName'] = {'nameUse': 'public',
                                                  'forename': pseudonym['forename'],
                                                  'surname': pseudonym['surname']}
                    identities[id]['isRelated'].append(is_related)
            identities[id]['personalNameVariant'] = []    
            for vn in names[id]['variant names']:
                variant_name = {'nameUse': 'public'}
                variant_name.update(vn)
                identities[id]['personalNameVariant'].append(variant_name)
            if names[id]['DATE_OF_BIRTH']:
                if self.validate_date(names[id]['DATE_OF_BIRTH']):
                    identities[id]['birthDate'] = names[id]['DATE_OF_BIRTH']
                else:
                    logging.error("Invalid date %s in record %s"%(names[id]['DATE_OF_BIRTH'], id))
            if names[id]['DATE_OF_DEATH']:
                if self.validate_date(names[id]['DATE_OF_DEATH']):
                    identities[id]['deathDate'] = names[id]['DATE_OF_DEATH']
                else:
                    logging.error("Invalid date %s in record %s"%(names[id]['DATE_OF_DEATH'], id))
            for group_name in names[id]['groups']:
                is_related = {'identityType': 'organisation', 'relationType': 'isMemberOf'}
                organisation_name = {'mainName': group_name}
                is_related['organisationName'] = organisation_name
                identities[id]['isRelated'].append(is_related)
            identities[id]['countriesAssociated'] = ['FI']
            identities[id]['resource'] = []
            person_id = id_dict.get(id, id)
            if person_id in tracks:
                matching_name = dict()
                if names[person_id]['pseudonyms'] and names[person_id]['artist names']:
                    if names[id]['type'] == 'pseud':
                        pseudonym = names[id]['pseudonyms'][0]
                        matching_name['forename'] = pseudonym['forename']
                        matching_name['surname'] = pseudonym['surname']
                    else:
                        matching_name['forename'] = names[id]['FIRST_NAME']
                        matching_name['surname'] = names[id]['LAST_NAME']
                for track in tracks[person_id]:
                    resource = {'title': track['TRACK_TITLE'],
                                'creationRole': 'prf',
                                'date': track['RECORDING_YEAR'],
                                'identifiers': {'ISRC': [track['ISRC']]}}
                    mainartist_name = {'forename': None, 'surname': None}
                    mainartist_names = track['MAINARTIST_NAME'].split(' ')
                    if len(mainartist_names) == 1:
                        mainartist_name['surname'] = mainartist_names[0]
                    else:
                        mainartist_name['surname'] = mainartist_names[-1]
                        mainartist_name['forename'] = ' '.join(mainartist_names[:-1])
                    if not matching_name or self.names_match(matching_name, mainartist_name):
                        identities[id]['resource'].append(resource)

                identities[id]['resource'] = identities[id]['resource'][:max_number_of_titles]


        count = 0
        for id in identities:
            
            if not identities[id].get('resource'):
                count += 1

        for id in deletable_identities:
            if id in identities:
                del(identities[id])
        identities = dict(sorted(identities.items()))

        print("Performers without records"+str(count))
        return identities