import unittest
import configparser
import json
from tools import api_query

class APIQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read('tests/config.ini')
        cls.sru_api_query = api_query.APIQuery(config_section=cls.config['SRU API'])
    

    """ def test_add_query_parameters(self):
        self.sru_api_query.add_query_parameters(query, name, value)
    """
    def test__form_query_url(self):
        # Test SRU API searches
        parameters = {'maximumRecords': '100'}
        query_strings = "cn=nlfin and not cn=isni"   
        query_url = self.sru_api_query._form_query_url(query_strings, additional_parameters=parameters)
        self.assertEqual("http:xxxxx.xxxx?query=cn%3Dnlfin+and+not+cn%3Disni&maximumRecords=100&operation=searchRetrieve", query_url)
        query_strings = "cn=nlfin and upd:2024"
        query_url = self.sru_api_query._form_query_url(query_strings, additional_parameters=parameters)
        self.assertEqual("http:xxxxx.xxxx?query=cn%3Dnlfin+and+upd%3A2024&maximumRecords=100&operation=searchRetrieve", query_url)
        query_strings = 'cn="VIAF 77823260"'
        query_url = self.sru_api_query._form_query_url(query_strings, additional_parameters=parameters)
        self.assertEqual("http:xxxxx.xxxx?query=cn%3D%22VIAF+77823260%22&maximumRecords=100&operation=searchRetrieve", query_url)
        # Test OAI-PMH searches
        self.sru_api_query.constant_parameters = dict()
        query_strings = None
        parameters = {'metadataPrefix': 'marc', 'verb': 'ListRecords', 'from': '2023-08-24', 'until': '2023-08-26'}
        query_url = self.sru_api_query._form_query_url(query_strings, additional_parameters=parameters)
        self.assertEqual("http:xxxxx.xxxx?metadataPrefix=marc&verb=ListRecords&from=2023-08-24&until=2023-08-26", query_url)
        parameters = {'verb': 'GetRecord', 'identifier': 'doi:10234.123453/45654FASDFDDF'}
        query_url = self.sru_api_query._form_query_url(query_strings, additional_parameters=parameters)
        self.assertEqual("http:xxxxx.xxxx?verb=GetRecord&identifier=doi%3A10234.123453%2F45654FASDFDDF", query_url)

if __name__ == "__main__":
    unittest.main()