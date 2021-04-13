import re
import sys
import os
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
                    self.get_record_data(record)

        logging.info("Resource records from file %s read"%input_file)          
                        
    def get_record_data(self, record, search_id=None):
        """
        Collects data for ISNI request from a bibliograpical MARC21 record 
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
                
                if field['b']:  
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
            if field['b']:
                publisher = field['b']
                title_of_work['publisher']  = self.trim_data(publisher)
            if title_of_work['publisher'] and field['c']:
                #Note: ISNI allows more than one date, but for sorting purposes the first valid year is chosen
                #publisher's name is mandatory, date of publication is no
                title_of_work['date'] = self.trim_year(field['c'])
        if not title_of_work['publisher']:
            for field in record.get_fields('264'):                
                if field['b']:
                    publisher = field['b']
                    title_of_work['publisher'] = self.trim_data(publisher)
                    if title_of_work['publisher'] and field['c']:
                        title_of_work['date'] = self.trim_year(field['c'])
        
        """
        valid identifiers for titles of work in ISNI: ISRC, ISWC, ISBN, ISSN, ISAN, ISTC, ISMN, DOI, OCN 
        """
        title_of_work['identifiers'] = {}
        title_of_work['identifiers']['ISBN'] = self.get_identifiers("ISBN", record, '020', 'a')
        title_of_work['identifiers']['ISSN'] = self.get_identifiers("ISSN", record, '022', 'a')
        title_of_work['identifiers']['ISRC'] = self.get_identifiers("ISRC", record, '024', 'a', '0')
        title_of_work['identifiers']['ISMN'] = self.get_identifiers("ISMN", record, '024', 'a', '2')
                
        #language code is needed for sorting titles:
        title_of_work['language'] = None
        for field in record.get_fields('041'):
            if uniform_title:
                if field['h']:
                    if self.validator.valid_language_code(field['h']):
                        title_of_work['language'] = field['h']
                    else:
                        logging.error("Invalid language code in record: %s"%record_id)
            else:
                #TODO: pick only the first language code if multiple subfields with same code? 
                if field['a']:
                    if self.validator.valid_language_code(field['a']):
                        title_of_work['language'] = field['a']
                    else:
                        logging.error("Invalid language code in record: %s"%record_id)
                 
        #fields in prefence order when adding creation roles:
        name_fields = ['100', '110', '700', '710']
        # TODO: matching with authors and works is done with names and ids in batch load
        # when matching is done with ids only, rewrite this section
        authors = {}
        #only one creation role (function) for one author:
        for tag in name_fields:
            for field in record.get_fields(tag):
                # TODO: temporary code to remove g subfields with "ennakkotieto"
                if field['g']:
                    return
                role = CREATION_ROLES[tag]
                function_code = None
                author_id = None
                for sf in field.get_subfields('0'):
                    #remove parenthesis and text inside, e. g. "(FIN11)":
                    author_id = re.sub("[\(].*?[\)]", "", sf)
                # TODO: remove this section after batch load
                if not author_id:
                    author_id = ""
                    subfield_codes = ['a', 'b', 'c', 'd', 'q']
                    for sc in subfield_codes:
                        for sf in field.get_subfields(sc):
                            if sf.endswith(","):
                                sf = sf[:-1]
                            author_id += sf
                    if author_id.endswith("."):
                        author_id = author_id[:-1]
               
                if author_id and author_id not in authors:
                    #only one creation role possible in ISNI, the first subfield e is chosen
                    if not(search_id and search_id != author_id):
                        if field['e']:
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
    
    def get_identifiers(self, identifier_type, record, field_code, subfield_code, indicator1 = " "):
        """

        :param identifier_type: Name of the identifier
        :param record: MARC21 record
        :param field_code: MARC field number from which identifier is fetched
        :param subfield_code: MARC subfield code from which identifier is fetched
        :param indicator1: first MARC field indicator, indicating identifier type
        """
        identifierValues = []
        for field in record.get_fields(field_code):
            if field.indicators[0] == indicator1:
                for sf in field.get_subfields(subfield_code):
                    sf = sf.replace('-', '')
                    valid = True
                    if identifier_type == "ISBN":
                        valid = self.validator.check_ISBN(sf)
                    if identifier_type == "ISSN":
                        valid = self.validator.check_ISSN(sf)
                    if identifier_type == "ISRC":
                        valid = self.validator.check_ISRC(sf)
                    if not valid:
                        logging.error("Invalid %s identifier in record: %s in field %s"%(identifier_type, record['001'].data, field))
                    else:
                        identifierValue = sf.replace('-','')
                        identifierValues.append(identifierValue)
        return identifierValues            
    
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
        