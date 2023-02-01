import logging
import os.path

class TermEncoder():
        
    def __init__(self):    
        loglevel = logging.INFO
        logger = logging.getLogger()
        logger.setLevel(loglevel)

        directory = os.path.realpath(os.path.join(os.path.dirname(__file__)))
        data_directory = os.path.join(directory, 'data')
        code_files = {
            'country codes': 'country_codes.dat',
            'organisation types': 'organisation_types.dat',
            'function codes': 'function_codes.dat',
            'person relation types': 'person_relation_types.dat',
            'organisation relation types': 'organisation_relation_types.dat'
        }
        self.code_dicts = {}
        for name in code_files:
            self.code_dicts[name] = {}

        for name in code_files:
            with open(os.path.join(data_directory, code_files[name]), 'r', encoding = 'utf-8') as fh:
                for line in fh:
                    try:
                        values = line.strip().split(';', 1)
                        self.code_dicts[name][values[0]] = values[1]
                    except TypeError:
                        logging.info("Values %s missing in dictionary %s"%(line, name))
                    except IndexError:
                        logging.info("Value of a key %s missing in dictionary %s"%(line, name))

    def encode_term(self, term, code_dict_name):
        if term in self.code_dicts[code_dict_name]:
            return self.code_dicts[code_dict_name][term]
        if code_dict_name == "organisation types":
            return "Other to be defined"
        
    def get_country_codes(self):
        return self.code_dicts['country codes']