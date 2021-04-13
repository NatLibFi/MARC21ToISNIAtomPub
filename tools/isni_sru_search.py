import re
import io
import os
import logging
import requests
#from manage_isni_responses import ISNIResponse
#from database_handler import DatabaseHandler
#from get_isni_metadata import get_metadata_from_files

#GLOBAL_VARIABLES
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


#SRU API query parameters:
BASEURL = 'https://isni-m.oclc.org/sru' 
DATABASE = 'DB=1.3' 
OPERATION = 'operation=searchRetrieve'
RECORD_SCHEMA = 'recordSchema=isni-e' #provisional records visible in this schema
USERNAME  = 'username=' + os.environ['ISNI_USER']
PASSWORD = 'password=' + os.environ['ISNI_PASSWORD']

def get_ids_from_file(file_path):
    # opens a text file with one id in one row
    ids = []
    with open(file_path, 'r', encoding = 'utf-8') as fh:
        for line in fh:
            id = line.rstrip()
            ids.append(id)
    return ids

def form_query(parameters):
    query = ""
    for parameter in parameters:
        if query:
            query += "+and+"
        values = parameters[parameter]
        #insert quotes if any special character in parameter values:
        if any(char in URL_ENCODINGS for char in values):
            values = "\"" + values + "\""
        query += 'pica.' + \
                parameter + \
                "+" + encode_chars("=") + "+" + \
                encode_chars(values)      
    return query

def encode_chars(string):
    encoded_string = ""
    for s in string:
        if s in URL_ENCODINGS:
            encoded_string += URL_ENCODINGS[s]
        else:
            encoded_string += s
    return encoded_string

def form_query_url(query, additional_parameters=None):
    """
    query: query parameters formatted with URL encodings using form_query function
    additional_parameters: e.g. maximumRecords=20
    """
    query = 'query=' + query
    url = BASEURL + '/' + \
            USERNAME + '/' + \
            PASSWORD + '/' + \
            DATABASE + '/?' + \
            query + '&' + \
            OPERATION + '&' + \
            RECORD_SCHEMA
    if additional_parameters:
        for ap in additional_parameters:
            url += "&" + ap
    return url 

def get_isni_with_ppn(ppn):
    parameters = {"ppn": ppn}
    query = form_query(parameters)
    url = form_query_url(query)
    r = requests.get(url)
    return r.text
    
def get_data_with_contributor_identifiers(contributor_identifiers, output_directory, contributor_id):
    """
    sends multiple ISNI AtomPub requests with contributor's identifiers, response XML files are automatically named with id
    contributor_numbers: contributors ids 
    output_directory: output directory for requested ISNI records
    contributor_id: contrib in ISNI database (e. g. NLFIN)
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    for cn in contributor_identifiers:
        #remove parenthese and hyphens from identifier e.g. (FI-ASTERI-N)
        cn = cn.replace("(", " ")
        cn = cn.replace(")", " ")
        cn = cn.replace("-", "")
        parameters = {"cn": contributor_id + " " + cn}
        query = form_query(parameters)
        url = form_query_url(query)
        file_path = os.path.join(output_directory, cn + ".xml")
        r = requests.get(url)
        return r.text

def write_request(file_path, url):
    r = requests.get(url)
    with open(file_path, 'w', encoding="latin1") as f:
        f.write(r.text)
    
def isni_sru_search(query_parameters, additional_parameters=None):
    query = form_query(query_parameters)
    url = form_query_url(query, additional_parameters)
    r = requests.get(url)
    return r.text
