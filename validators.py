import re
import logging
import os.path
from term_encoder import TermEncoder

record_types = ['a', 'c', 'd', 'e', 'f', 'g', 'i', 'j', 'k', 'm', 'o', 'p', 'r', 't'] 
bibliographic_levels = ['a', 'b', 'c', 'd', 'i', 'm', 's']
                 
class Validator:

    def __init__(self):
        self.term_encoder = TermEncoder()
        code_files = {
            'country': 'country_codes.dat',
            'language': 'language_codes.dat'
        }
        self.codes = {}
        directory = os.path.realpath(os.path.join(os.path.dirname(__file__)))
        data_directory = os.path.join(directory, 'data')
        for cf in code_files:
            self.codes[cf] = set()
            with open(os.path.join(data_directory, code_files[cf]), 'r', encoding = 'utf-8') as fh:
                for line in fh:
                    try:
                        if cf == 'country':
                            values = line.strip().split(';', 1)
                            self.codes[cf].add(values[1])
                        else:
                            self.codes[cf].add(line.rstrip())
                    except TypeError:
                        logging.info("Values %s missing in %s code dictionary"%(line, cf))
                    except IndexError:
                        logging.info("Value of a key %s missing in %s code dictionary"%(line, cf))
        
    def get_creation_class(self, leader):
        #check that creation class in record leader has valid MARC21 codes
        if len(leader) == 24:
            if leader[6] in record_types and leader[7] in bibliographic_levels:
                return leader[6] + leader[7]
        
    def check_ISBN(self, isbn):    
        isbn = isbn.replace('-','')
        checksum = 0
        if len(isbn) == 10:
            for x in range(1, 11):
                digit = 0
                try:
                    if x == 10 and isbn[x - 1] == "X":
                        digit = 10
                    else:
                        digit = int(isbn[x - 1])
                except ValueError:
                    return False
                checksum += digit * x
            if checksum % 11 == 0:
                return True
            else:
                return False
        elif len(isbn) == 13:
            for x in range(1, 14):
                try:
                    digit = int(isbn[x - 1])
                except ValueError:
                    return False
                if x % 2 == 0:
                    checksum += digit * 3
                else:
                    checksum += digit
            if checksum % 10 == 0:
                return True
            else:
                return False
        else:
            return False
            
    def check_ISSN(self, issn): 
        issn = issn.replace('-','')
        if len(issn) == 8:
            checkdigit = 0
            try:
                if issn[7] == "X":
                    checkdigit = 10
                else:
                    checkdigit = int(issn[7])
            except ValueError:
                return False
            factor = 8  
            checksum = 0
            for x in range(0, 7):
                try:
                    checksum += int(issn[x]) * factor
                    factor -= 1
                except ValueError:
                    return False
            if checksum % 11 == 0 and checkdigit == 0:
                return True
            if 11 - checksum % 11 == checkdigit:
                return True
            else:
                return False
        else:
            return False

    def check_ISRC(self, isrc):
        #validation instructions from: http://isrc.ifpi.org/en/isrc-standard/code-syntax
        isrc = isrc.replace('-','')
        additional_country_codes = ['BX', 'BC', 'FX', 'QM', 'QZ', 'DG', 'UK', 'TC', 'CP', 'DG', 'ZZ', 'CS', 'YU']
        if len(isrc) != 12:
            return False
        if isrc[0:2] not in self.codes['country'].union(additional_country_codes):
            return False
        if not isrc[5:12].isdigit():
            return False
        if not re.match('^[A-Z0-9]+$', isrc[2:5]):
            return False
        return True

    def valid_ISNI_checksum(self, identifier):
        numbers = identifier.replace('-', '')
        if len(numbers) != 16:
            return None
        checksum = numbers[15]
        if checksum == "X":
            checksum = "10"
        numbers = numbers[:15]
        if not numbers.isdigit() or not checksum.isdigit():
            return None
        sum = 0
        for n in numbers:
            sum = (sum * 2) + int(n)
        sum = (sum * 2) + int(checksum)
        if sum % 11 == 1:
            return identifier
        else:
            return None

    def valid_ORCID(self, identifier):
        identifier = identifier.replace('https://orcid.org/', '')
        pattern = re.compile(r'\d{4}-\d{4}-\d{4}-\d{3}[0-9X]')
        if not pattern.fullmatch(identifier):
            return None
        return self.valid_ISNI_checksum(identifier)

    def format_year(self, year):
        if year.isdigit():
            if len(year) < 4:
                year = "0" * (4 - len(year)) + year
            if len(year) == 4:
                return year
        elif len(year) == 6:
            return year[:3] + "-" + year[4:]
        elif len(year) == 8:
            return year[:3] + "-" + year[4:5] + "-" + year[6:]
        else:
            return year
            
    def valid_language_code(self, code):
        if code in self.codes['language']:
            return True
        else:
            return False  
            
    def valid_country_code(self, code):        
        if code in self.codes['country']:
            return True
        else:
            return False 