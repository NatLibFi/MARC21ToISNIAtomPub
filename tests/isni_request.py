import unittest
import xml.etree.cElementTree as ET
from xml.dom.minidom import parseString
import re
from isni_request import create_xml

class ISNIRequestTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):   
        cls.organisation_xml = ET.parse("tests/000000001.xml")
        cls.person_xml = ET.parse("tests/000000002.xml")

        return super(ISNIRequestTest, cls).setUpClass()

    def test_request_organisation(self):
        data =  {'identifier': '(FI-ASTERI-N)0000000001',
                 'identityType': 'organisation',
                 'organisationName': {'mainName': 'Organisation name', 'subdivisionName': []},
                 'organisationNameVariant': [{'mainName': 'ABC', 'subdivisionName': []},
                                             {'mainName': 'ABC', 'subdivisionName': ['B', 'C']},
                                            ], 
                 'organisationType': 'Not for Profit Organization',
                 'ISNI': '0000000119405487',
                 'otherIdentifierOfIdentity': [],
                 'usageDateFrom': '1831',
                 'usageDateTo': '1999',
                 'languageOfIdentity': ['fin', 'swe'],
                 'countriesAssociated': ['AX', 'GB'],
                 'countryCode': 'FI',
                 'URI': ['https://fi.wikipedia.org/', 'https://www.fi'], 
                 'resource': [{'title': 'Title of Work', 
                               'creationClass': None,
                               'publisher': None,
                               'date': None,
                               'identifiers': {'ISBN': ['01001001010', '010101010'], 'ISSN': [], 'ISRC': [], 'ISMN': [], 'DOI': []},
                               'language': 'fin',
                               'creationRole': None,
                               },
                              {'title': 'Second Title of Work', 
                               'creationClass': 'am',
                               'publisher': 'PUBLISHER NAME',
                               'date': '1990',
                               'identifiers': {'ISBN': [], 'ISSN': [], 'ISRC': ['01001001010', '010101010'], 'ISMN': [], 'DOI': []},
                               'language': 'fin',
                               'creationRole': 'aut',
                               }
                 ],
                 'isRelated': [{'identityType': 'organisation',
                                'organisationName': {'mainName': 'DFG', 'subdivisionName': ['H']}, 
                                'ISNI': '0000000119405488',
                                'startDateOfRelationship': '1900',
                                'endDateOfRelationship': '2000',
                                'relationType': 'supersedes'},
                               {'identityType': 'personOrFiction',
                                'personalName': {'nameUse': 'public',
                                                 'surname': 'Smith',
                                                 'forename': 'S',
                                                 'numeration': None,
                                                 'nameTitle': None},
                                'relationType': 'hasMember'
                               }], 
                 'isNot': ['0000000119405488']
            }

        request = create_xml(data)
        request = re.sub('\s+(?=<)', '', request)
        test_xml = self.organisation_xml.getroot()
        test_xml = parseString(ET.tostring(test_xml, "utf-8")).toprettyxml()
        test_xml = re.sub('\s+(?=<)', '', test_xml)

        self.assertEqual(test_xml, request)

    def test_request_person(self):
        data =  {'identifier': '(FI-ASTERI-N)0000000002',
                 'identityType': 'personOrFiction',
                 'personalName': {'nameUse': 'public',
                                  'surname': 'Meikäläinen',
                                  'forename': 'Matti',
                                  'numeration': 'III',
                                  'nameTitle': 'prof.'},
                 'personalNameVariant': [],
                 'otherIdentifierOfIdentity': [], 
                 'birthDate': '1900', 
                 'deathDate': '2000', 
                 'dateType': 'circa', 
                 'languageOfIdentity': [], 
                 'countriesAssociated': ['AX', 'FI'],
                 'URI': ['https://fi.wikipedia.org/', 'https://www.fi'], 
                 'resource': [{'title': 'Second Title of Work', 
                               'creationClass': 'am',
                               'publisher': 'PUBLISHER NAME',
                               'date': '1990',
                               'identifiers': {'ISBN': [], 'ISSN': [], 'ISRC': ['01001001010', '010101010'], 'ISMN': [], 'DOI': []},
                               'language': 'fin',
                               'creationRole': 'aut',
                               }
                 ],
                 'isRelated': [{'identityType': 'organisation',
                                'organisationName': {'mainName': 'DFG', 'subdivisionName': ['H']}, 
                                'ISNI': '0000000119405488',
                                'startDateOfRelationship': '1900',
                                'endDateOfRelationship': '2000',
                                'relationType': 'isMemberOf'},
                               {'identityType': 'personOrFiction',
                                'personalName': {'nameUse': 'public',
                                                 'surname': 'Smith',
                                                 'forename': 'S',
                                                 'numeration': None,
                                                 'nameTitle': None},
                                'relationType': 'co-author'
                               }], 
                 'isNot': ['0000000119405488']
            }

        request = create_xml(data)
        request = re.sub('\s+(?=<)', '', request)
        test_xml = self.person_xml.getroot()
        test_xml = parseString(ET.tostring(test_xml, "utf-8")).toprettyxml()
        test_xml = re.sub('\s+(?=<)', '', test_xml)

        self.assertEqual(test_xml, request)

if __name__ == "__main__":
    unittest.main()
