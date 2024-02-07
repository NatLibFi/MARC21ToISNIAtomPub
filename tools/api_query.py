#!/usr/bin/env python3
import json
import os
import logging
import pickle
import requests
import sys
from tools import parse_isni_response
import time
import urllib

class APIQuery():

    def __init__(self, config_section, username=None, password=None):
        """
        class for initializing paramaters for OAI-PMH and SRU 1.1 searches
        : param config_section: a section of config file parsed by ConfigParser
        : param username: ISNI username
        : param password: ISNI password
        """   
        self.baseurl = config_section.get('baseurl')
        self.database = config_section.get('database', fallback=None)
        self.constant_parameters = dict()
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
            self.username = username
        if password:
            self.password = password

    def _form_query_url(self, query="", additional_parameters=dict()):
        """
        :param query: query string for sru search
        :param additional_parameters: dict with ISNI SRU search parameteters, e.g. {'maximumRecords': '100'}
        """
        url = self.baseurl
        if self.username and self.password:
            url += 'username=' + self.username + '/password=' + self.password + '/'
        if query:
            query = urllib.parse.quote_plus(query)
            url += '?query=' + query
        else:
            url += '?'
        for p in additional_parameters:
            if not url.endswith('?'):
                url += '&'
            url += p + '=' + urllib.parse.quote_plus(additional_parameters[p])
        for p in self.constant_parameters:
            if not url.endswith('?'):
                url += '&'
            url += p + '=' + urllib.parse.quote_plus(self.constant_parameters[p])

        return url

    def api_search(self, query="", parameters=None):
        """
        Forms query URL and sends OAI-PMH or SRU API request
        :param query: a string containing sru search query
        :param parameters: a dict of keyword arguments used as search parameters
        """
        url = self._form_query_url(query, parameters)
        try:
            r = requests.get(url, timeout=self.timeout)
        except requests.exceptions.ReadTimeout:
            logging.error("Timeout for query %s"%url)
            return

        return r.text

    def get_isni_query_data(self, query, query_file=None):
        """
        Performs ISNI SRU API queries and parses query data into dict
        :param query: a string containing ISNI sru search query
        :param record_schema: ISNI ARU API recordSchema ("isni-e" or "isni-b")
        :param query_file: File path to load/save query results in pickle file for possibly continuing interrupted queries
        """
        data = {}
        data = {'startRecord': 1, 'query': query, 'results': [], 'record number': 2}
        if query_file and os.path.isfile(query_file):
            with open(query_file, 'rb') as input_file:
                data = pickle.load(input_file)
                if data['query'] != query:
                    while True:
                        answer = input("Query result file exits with different query. Overwrite (Y/N)?")
                        if answer.lower() == "y":
                            break
                        if answer.lower() == "n":
                            sys.exit(2)
        startRecord = data['startRecord']
        if not data['record number']:
            logging.error("Query %s results missing number of records"%data['query'])
        else:
            while startRecord <= data['record number']:
                data['startRecord'] = startRecord
                additional_parameters = {'maximumRecords': '100', 'startRecord': str(startRecord)}
                url = self._form_query_url(query, additional_parameters)
                try:
                    results = requests.get(url, timeout=self.timeout).text
                except requests.exceptions.ReadTimeout:
                    logging.error("Timeout for query %s"%url)
                data['record number'] = parse_isni_response.get_number_of_records(results)
                data['results'].extend(parse_isni_response.dictify_xml(results))
                if query_file:
                    with open(query_file, 'wb') as output:
                        pickle.dump(data, output, pickle.HIGHEST_PROTOCOL)
                if not data['record number']:
                    break
                if data['record number'] > 100:
                    logging.info("Querying records from number %s"%startRecord)
                    time.sleep(1)
                startRecord += 100

        return data['results']
