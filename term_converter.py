import logging

class TermEncoder():
        
    def __init__(self):    
        self.type_dict = {}    
        self.country_code_dict = {}
        self.function_code_dict = {}
        
        with open("organisation_types.dat", 'r', encoding = 'utf-8') as fh:
            for line in fh:
                try:
                    values = line.strip().split(';', 1)
                    self.type_dict.update({values[0]: values[1]})
                except TypeError:
                    logging.error("Organisation type missing in dictionary")
        with open("country_codes.dat", 'r', encoding = 'utf-8') as fh:
            for line in fh:
                try:
                    values = line.strip().split(';', 1)
                    self.country_code_dict.update({values[0]: values[1]})
                except TypeError:
                    logging.error("Value missing in dictionary")    
        with open("function_codes.dat", 'r', encoding = 'utf-8') as fh:
            for line in fh:
                try:
                    values = line.strip().split(';', 1)
                    self.function_code_dict.update({values[0]: values[1]})
                except TypeError:
                    logging.error("Value missing in dictionary")                
                
    def convert_organisation_type(self, organisation_type):
        if organisation_type in self.type_dict.keys():
            return self.type_dict[organisation_type]
        return "Other to be defined"
    
    def convert_to_function_code(self, creation_role):
        if creation_role in self.function_code_dict.keys():
            return self.function_code_dict[creation_role]
        return
                
    def convert_to_country_code(self, country_name):                           
        if country_name in self.country_code_dict.keys():
            return self.country_code_dict[country_name]
        return
        
    def get_country_codes(self):
        return self.country_code_dict