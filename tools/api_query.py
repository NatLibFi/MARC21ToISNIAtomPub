import logging
import requests
import json
import sys

URL_ENCODINGS = {
    '!': '%21',
    '#': '%23',
    '$': '%24',
    '&': '%26',
    '\'': '%27',
    '(': '%28',
    ')': '%29',
    '*': '%2A',
    '+': '%2B',
    ',': '%2C',
    '/': '%2F',
    ': ': '%3A',
    ';': '%3B',
    '=': '%3D',
    '?': '%3F',
    '@': '%40',
    '[': '%5B',
    ']': '%5D',
    ' ': '%20',
    '\"': '%22',
    '%': '%25',
    '-': '%2D',
    '.': '%2E',
    '<': '%3C',
    '>': '%3E',
    '\\': '%5C',
    '^': '%5E',
    '_': '%5F',
    '`': '%60',
    '{': '%7B',
    '|': '%7C',
    '}': '%7D',
    '~': '%7E'
}  

def encode_chars(string):
    encoded_string = ""
    for s in string:
        if s in URL_ENCODINGS:
            encoded_string += URL_ENCODINGS[s]
        else:
            encoded_string += s
    return encoded_string

class APIQuery():
    def __init__(self, config_section, username=None, password=None):
        """
        class used to make SRU API searches
        : param config_section: a section of config file parsed by ConfigParser
        : param username: ISNI username
        : param password: ISNI password
        """   
        self.baseurl = config_section.get('baseurl')
        self.database = config_section.get('database', fallback=None)
        self.constant_parameters = None
        try:
            if config_section.get('parameters'):
                self.constant_parameters = json.loads(config_section.get('parameters'))
        except json.decoder.JSONDecodeError as e:
            logging.error("Parameters %s malformatted in config.ini"%config_section.get('parameters'))
            logging.error(e)
            sys.exit(2)
        self.username = None
        self.password = None
        self.timeout = int(config_section.get('timeout'))
        if username:
            self.username = 'username=' + username
        if password:
            self.password = 'password=' + password

    def form_isni_query(self, parameters):
        query = ""
        for parameter in parameters:
            if query:
                query += "+and+"
            values = parameters[parameter]
            #if query strings contain whitespace, it must be surrounded by quotations according to ISNI SRU API guidelines 
            if " " in values:
                values = "\"" + values + "\""
            values = values.replace(" ", "+")
            query += 'pica.' + \
                    parameter + \
                    "+" + encode_chars("=") + "+" + \
                    encode_chars(values)  
        return query

    def add_query_parameters(self, query, name, value):
        if not query.endswith('?'):
            query += "&"
        return query + name + "=" + value

    def form_query_url(self, query_strings, parameters=None, token_parameters=None):
        """
        query: query parameters formatted with URL encodings using form_query function
        additional_parameters: SRU API parameters in dict as keys and values, e. g. {'maximumRecords': '20'}
        isni_query: if true, format query for ISNI SRU API
        """
        url = self.baseurl
        for param in [self.username, self.password, self.database]:
            if param:
                if not url.endswith('/'):
                    url += '/'
                url += param
        url += "?"
        if query_strings:
            query_string = ""
            for query in query_strings:                
                query_string += query
            query_string = query_string.replace(' ', '%20')
            url = self.add_query_parameters(url,
                                            'query',
                                            query_string)
        if self.constant_parameters:
            for parameter in self.constant_parameters:
                url = self.add_query_parameters(url,
                                                parameter,
                                                self.constant_parameters[parameter])
        if parameters:
            for parameter in parameters:
                url = self.add_query_parameters(url,
                                                parameter,
                                                parameters[parameter])
        if token_parameters:
            for parameter in token_parameters:
                url = self.add_query_parameters(url,
                                                parameter,
                                                encode_chars(token_parameters[parameter]))

        return url

    def search_with_id(self, id_type, identifier):
        """
        sends multiple ISNI AtomPub requests with contributor's identifiers, response XML files are automatically named with id
        :param contributor_numbers: contributors ids 
        :param output_directory: output directory for requested ISNI records
        :param contributor_id: contrib in ISNI database (e. g. NLFIN)

        """
        parameters = {id_type: identifier}
        query = self.form_isni_query(parameters)
        url = self.form_query_url([query])
        try:
            r = requests.get(url, timeout=self.timeout)
        except requests.exceptions.ReadTimeout:
            logging.error("Timeout for query %s"%url)
            return
        return r.text
        
    def get_data_with_local_identifiers(self, contributor_number, contributor_identifier, source_code):
        """
        sends multiple ISNI AtomPub requests with contributor's identifiers, response XML files are automatically named with id
        :param contributor_number: contributor's source code in ISNI database (e. g. NLFIN)
        :param contributor_identifier: local identifier of contributors 
        :param source_code: contributor's database identifier in ISNI database (e. g. FI-ASTERI-N)
        """
        source_code = source_code.replace("(", " ")
        source_code = source_code.replace(")", " ")
        source_code = source_code.replace("-", "")

        parameters = {"cn": contributor_number + " " + source_code + " " + contributor_identifier}
        query = self.form_isni_query(parameters)
        url = self.form_query_url([query])
        r = requests.get(url, timeout=self.timeout)
        return r.text

    def write_request(self, url, file_path):
        r = requests.get(url, timeout=self.timeout)
        with open(file_path, 'w', encoding="latin1") as f:
            f.write(r.text)

    def api_search(self, query_strings=None, parameters=None, token_parameters=None):
        url = self.form_query_url(query_strings, parameters, token_parameters)
        try:
            r = requests.get(url, timeout=self.timeout)
        except requests.exceptions.ReadTimeout:
            logging.error("Timeout for query %s"%url)
            return
        return r.text