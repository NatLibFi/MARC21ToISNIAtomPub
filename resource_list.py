import re
import sys
import logging
import copy
from term_encoder import TermEncoder
from validators import Validator
from pymarc import MARCReader
from tools import aleph_seq_reader

#creation roles used for sorting and selecting titles for an author, importance of role in alphabetical order:  
CREATION_ROLES = {
    "100": "author",
    "110": "author",
    "600": "subject",
    "610": "subject",
    "700": "contributor",
    "710": "contributor"
}

class ResourceList:
    
    def __init__(self, input_file=None, format=None):
        """
        Converts MARC21 bibliographic records into a dict object containing relevant data for ISNI request
        For faster execution crop MARC21 fields from file except 
        leader, 001, 020, 022, 024, 041, 100, 110, 240, 245, 260, 264, 600, 610, 700, 710
        :param input_file: file containing MARC21 bibliographical records
        :param format: format of input_file, either 'marc21' or 'alephseq'
        """
        self.validator = Validator()
        self.term_encoder = TermEncoder()
        self.titles = {}

        if input_file:
            logging.info('Loading titles...')
            
            if format == "marc21":                       
                reader = MARCReader(open(input_file, 'rb'), to_unicode=True)
            elif format == "alephseq":                    
                reader = aleph_seq_reader.AlephSeqReader(open(input_file, 'r', encoding="utf-8"))
            else:
                logging.error("Not valid format to convert from: "%format)
                sys.exit(2)
            record = ""
            while record is not None:
                try:
                    record = next(reader, None)
                except Exception as e:
                    logging.exception(e)
                    continue
                if record:
                    self.add_record_data(record)
            reader.close()
            logging.info("Resource records from file %s read"%input_file)

    def add_record_data(self, record, search_id=None):
        """
        Adds data for ISNI request from a bibliograpical MARC21 record to titles dict
        :param record: bibliographical MARC21 record 
        :param search_id: get record information of only one author with this id
        """
        title_of_work = {} 
        if record['001']:
            record_id = record['001'].data
        else:
            return
        title = None
        uniform_title = None
        for field in record.get_fields('240'):
            for sf in field.get_subfields('a'):
                title = self.trim_data(sf)
                uniform_title = title
        if not title:
            for field in record.get_fields('245'):
                try:
                    for sf in field.get_subfields('a'):
                        title = self.trim_data(sf)
                except AttributeError:
                    logging.error("Attribute error in field 245 in bibliographical record: %s"%record_id)
                    return
                except ValueError:    
                    logging.error("Value error in field 245 in bibliographical record: %s "%record_id)
                    return

                if not title:
                    logging.error("Bibliographical record %s has not a title"%record_id)
                    return
                
                if 'b' in field and field['b']:
                    title += " " + field['b']
                    title = self.trim_data(title)
        if title:
            title_of_work['title'] = title
        if hasattr(record, 'leader'):
            leader = record.leader
        else:
            logging.error("Bibliographical record %s has not a leader"%record_id)
            return
        creation_class = self.validator.get_creation_class(leader)
        title_of_work['creationClass'] = creation_class
        if not creation_class:
            logging.error("Bibliographical record %s leader is invalid"%record_id)
        title_of_work['publisher'] = None
        title_of_work['date'] = None
        for field in record.get_fields('260'):    
            if 'b' in field and field['b']:
                if not "tuntematon" in field['b']:
                    publisher = field['b']
                    title_of_work['publisher']  = self.trim_data(publisher)
            if title_of_work['publisher'] and 'c' in field:
                #Note: ISNI allows more than one date, but for sorting purposes the first valid year is chosen
                #publisher's name is mandatory, date of publication is not
                trimmed_date = self.trim_year(field['c'])
                if trimmed_date:
                    title_of_work['date'] = trimmed_date
        if not title_of_work['publisher']:
            for field in record.get_fields('264'):                
                if 'b' in field and field['b']:
                    if not "tuntematon" in field['b']:
                        publisher = field['b'] 
                        title_of_work['publisher'] = self.trim_data(publisher)
                        if title_of_work['publisher'] and 'c' in field:
                            trimmed_date = self.trim_year(field['c'])
                            if trimmed_date:
                                title_of_work['date'] = trimmed_date
        
        title_of_work['identifiers'] = self.get_identifiers(record)

        #language code is needed for sorting titles:
        title_of_work['language'] = None
        for field in record.get_fields('041'):
            if uniform_title:
                if 'h' in field:
                    if self.validator.valid_language_code(field['h']):
                        title_of_work['language'] = field['h']
                    else:
                        logging.error("Invalid language code in record: %s"%record_id)
            else:
                # Note: multiple subfields with language codes possible
                if 'a' in field:
                    if self.validator.valid_language_code(field['a']):
                        title_of_work['language'] = field['a']
                    else:
                        logging.error("Invalid language code in record: %s"%record_id)
                 
        #fields in prefence order when adding creation roles:
        name_fields = ['100', '110', '700', '710']
        # when matching is done with ids only, rewrite this section
        authors = {}
        #only one creation role (function) for one author:
        for tag in name_fields:
            for field in record.get_fields(tag):
                role = CREATION_ROLES[tag]
                function_code = None
                author_id = None
                for sf in field.get_subfields('0'):
                    #remove parenthesis and text inside, e. g. "(FIN11)":
                    author_id = re.sub("[\(].*?[\)]", "", sf)
               
                if author_id and author_id not in authors:
                    #only one creation role possible in ISNI, the first subfield e is chosen
                    if not(search_id and search_id != author_id):
                        if 'e' in field and field['e']:
                            f = field['e']
                            if f.endswith(",") or f.endswith("."):
                                f = f[:-1]
                            function_code = self.term_encoder.encode_term(f, "function codes")
                            if not function_code:
                                logging.error("Creation role %s in record: %s missing from creation role file"%(field['e'], record_id))
                        authors[author_id] = {"creationRole": function_code, "role": role}

        if title:
            for author_id in authors:
                title_copy = copy.copy(title_of_work)
                title_copy.update(authors[author_id])
                if author_id in self.titles:
                    self.titles[author_id].append(title_copy)
                else:
                    self.titles[author_id] = [title_copy]
    
    def get_identifiers(self, record):
        """
        Get resource identifiers from MARC21 fields
        valid identifiers for titles of work in ISNI: ISRC, ISWC, ISBN, ISSN, ISAN, ISTC, ISMN, DOI, OCN
        :param record: MARC21 record
        """
        identifier_values = {}
        identifier_fields = ['020', '022', '024']
        for tag in identifier_fields:
            for field in record.get_fields(tag):
                for sf in field.get_subfields('a'):
                    identifier_type = None
                    if tag == '020':
                        identifier_type = 'ISBN'
                    if tag == '022':
                        identifier_type = 'ISSN'
                    if tag == '024':
                        if field.indicators[0] == '0':
                            identifier_type = 'ISRC'
                        if field.indicators[0] == '2':
                            identifier_type = 'ISMN'
                        if field.indicators[0] == '7':
                            if '2' in field and field['2'] == 'doi':
                                identifier_type = 'DOI'
                    if identifier_type:
                        if identifier_type in ['ISBN', 'ISSN', 'ISRC']:
                            sf = sf.replace('-', '')
                            valid = False
                            if identifier_type == 'ISBN':
                                # same ISBN validation for ISMN
                                valid = self.validator.check_ISBN(sf)
                            if identifier_type == 'ISSN':
                                valid = self.validator.check_ISSN(sf)
                            if identifier_type == 'ISRC':
                                valid = self.validator.check_ISRC(sf)
                            if valid:
                                if identifier_type not in identifier_values:
                                    identifier_values[identifier_type] = []
                                identifier_values[identifier_type].append(sf)
                            else:
                                logging.error("Invalid %s in record: %s in field %s"%(identifier_type, record['001'].data, field))
                        else:
                            if identifier_type == 'ISMN':
                                sf = sf.replace('-', '')
                            if identifier_type not in identifier_values:
                                identifier_values[identifier_type] = []
                            identifier_values[identifier_type].append(sf)

        return identifier_values
    
    def trim_year(self, year):
        """
        Trims MARC21 data from field 260 or 264
        :param year: MARC21 string data from a single subfield from field 260 or 264
        """
        replacable_chars = ['[', ']', '.', 'c', '(', ')']
        for rc in replacable_chars:
            year = year.replace(rc, '')
        year = year.strip()
        pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
        if pattern.fullmatch(year):
            return year
        return None    
    
    def trim_data(self, data):
        """
        Trims MARC21 hyphenation from MARC data
        :param data: MARC21 string data from a single subfield 
        """
        # remove data that has corrupted characters
        if "�" in data:
            return
        data = data.replace('[','').replace(']','')
        if data.endswith(',') or data.endswith("/") or data.endswith("."):
            data = data[:-1]
        data = data.strip()
        return data
        