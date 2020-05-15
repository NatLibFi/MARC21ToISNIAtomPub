import re
import sys
import logging
from term_encoder import TermEncoder
from validators import Validator

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
    
    def __init__(self, input_file, format):
        """
        Converts MARC21 bibliographic records into a dict object containing relevant data for ISNI request
        For faster execution crop MARC21 fields from file except 
        leader, 001, 020, 022, 024, 041, 100, 110, 240, 245, 260, 264, 600, 610, 700, 710

        """
        self.validator = Validator()
        self.term_encoder = TermEncoder()
        self.titles = {}
        
        logging.info('Loading titles...')
        if format == "marc21":
            from pymarc import MARCReader                         
            reader = MARCReader(open(input_file, 'rb'), to_unicode=True)
        elif format == "alephseq":
            from tools import aleph_seq_reader                       
            reader = aleph_seq_reader.AlephSeqReader(open(input_file, 'r', encoding="utf-8"))
        else:
            logging.error("Not valid format to convert from: "%format)
            sys.exit(2)
        self.read_records(reader)
        logging.info("Resource records from file %s read"%input_file)          
  
    def read_records(self, reader):
        """
        :param reader: MARCReader from pymarc library 
        """
        record = ""
        while record is not None:
            try:
                record = next(reader, None)
            except Exception as e:
                logging.exception(e)
                continue
            #one record may contain several authors with different functions
            #try:
            if record:
                titledata = self.get_record_data(record)
                if titledata:
                    title = {}
                    title.update({'title': titledata['title']})
                    """
                    publisher is mandatory in ISNI imprint element and year is not,
                    however add year if available for sorting titles by year
                    """
                    if 'creationClass' in titledata:
                        title.update({'creationClass': titledata['creationClass']})
                    if 'publisher' in titledata:
                        title.update({'publisher': titledata['publisher']})
                    if 'date' in titledata:
                        title.update({'date': titledata['date']})
                    if 'identifiers' in titledata:
                        title.update({'identifiers': titledata['identifiers']})
                    if 'language' in titledata:
                        title.update({'language': titledata['language']})
               
                    for author in titledata['authors']:                    
                        #checks if author has functions:
                        authortitle = dict(title)
                        if titledata['authors'][author]:
                            #choose only one function from the title for one author (ISNI request maximum):
                            authortitle.update({'creationRole': titledata['authors'][author]['function']})
                            #one of CREATION_ROLES
                            authortitle.update({'role': titledata['authors'][author]['role']})  
                        if author in self.titles:
                            self.titles[author].append(authortitle)
                        else:
                            titlelist = []
                            titlelist.append(authortitle)
                            self.titles[author] = titlelist
            

            
    def get_record_data(self, record):
        authors = {}
        title_of_work = {} 
        creationClass = None
        publisher = None
        title = None
        uniform_title = None
        date = None
        identifiers = {}
        language = None
        if record['001']:
            record_id = record['001'].data
        else:
            return

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
        if hasattr(record, 'leader'):
            leader = record.leader
        else:
            logging.error("Bibliographical record %s has not a leader"%record_id)
            return
        creationClass = self.validator.get_creation_class(leader)
        if not creationClass:
            logging.error("Bibliographical record %s leader is invalid"%record_id)
        
        for field in record.get_fields('260'):    
            if field['b']:
                publisher = field['b']
                publisher = self.trim_data(publisher)
            if publisher and field['c']:
                #Note: ISNI allows more than one date, but for sorting purposes the first valid year is chosen
                #publisher's name is mandatory, date of publication is no
                date = self.trim_year(field['c'])
        if not publisher:
            for field in record.get_fields('264'):                
                if field['b']:
                    publisher = field['b']
                    publisher = self.trim_data(publisher)
                    if publisher and field['c']:
                        date = self.trim_year(field['c'])
        
        """
        valid identifiers for titles of work in ISNI: ISRC, ISWC, ISBN, ISSN, ISAN, ISTC, ISMN, DOI, OCN 
        """
        identifiers.update({"ISBN": self.get_identifiers("ISBN", record, '020', 'a')})
        identifiers.update({"ISSN": self.get_identifiers("ISSN", record, '022', 'a')})
        identifiers.update({"ISRC": self.get_identifiers("ISRC", record, '024', 'a', '0')})
        identifiers.update({"ISMN": self.get_identifiers("ISMN", record, '024', 'a', '2')})
                
        #language code is needed for sorting titles:
        for field in record.get_fields('041'):
            if uniform_title:
                if field['h']:
                    if self.validator.valid_language_code(field['h']):
                        language = field['h']
                    else:
                        logging.error("Invalid language code in record: %s"%record_id)
            else:
                #TODO: pick only the first language code if multiple subfields with same code? 
                if field['a']:
                    if self.validator.valid_language_code(field['a']):
                        language = field['a']
                    else:
                        logging.error("Invalid language code in record: %s"%record_id)
                            
        #fields in prefence order when adding creation roles:
        name_fields = ['100', '110', '700', '710']
        # TODO: matching with authors and works is done with names and ids in batch load
        # when matching is done with ids only, rewrite this section

        #only one creation role (function) for one author:
        for tag in name_fields:
            for field in record.get_fields(tag):
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
                    if field['e']:
                        function_code = self.trim_line_end(field['e'])
                        function_code = self.term_encoder.encode_term(function_code, "function codes")
                        if not function_code:
                            logging.error("Creation role %s in record: %s missing from creation role file"%(field['e'], record_id))
                    authors.update({author_id: {"function": function_code, "role": role}})
        
        title_of_work['authors'] = authors
        title_of_work['title'] = title
        title_of_work['creationClass'] = creationClass
        title_of_work['publisher'] = publisher
        title_of_work['date'] = date
        ids = {}
        for identifier in identifiers:
            if identifiers[identifier]:
                ids.update({identifier: identifiers[identifier]})
        title_of_work['identifiers'] = ids
        title_of_work['language'] = language 
        if title:
            return title_of_work
    
    def trim_line_end(self, end):
        end = str(end)
        if end.endswith(","):
            end = end[:-1]
        if end.endswith("."):
            end = end[:-1]
        return end
    
    def get_identifiers(self, identifier_type, record, field_code, subfield_code, indicator1 = " "):
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
        replacable_chars = ['[', ']', '.', 'c', '(', ')']
        for rc in replacable_chars:
            year = year.replace(rc, '')
        year = year.strip()
        pattern = re.compile(r'-?\d{4}-\d{2}-\d{2}|-?\d{4}-\d{2}|-?\d{4}')
        if pattern.fullmatch(year):
            return year
        return None    
    
    def trim_data(self, data):
        # remove data that has corrupted characters
        if "�" in data:
            return
        data = data.replace('[','').replace(']','')
        if data.endswith(',') or data.endswith("/") or data.endswith("."):
            data = data[:-1]
        data = data.strip()
        return data
        