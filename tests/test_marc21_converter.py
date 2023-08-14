import unittest
import shutil
import sys
from unittest import mock
from converter import Converter
from resource_list import ResourceList
from pymarc import Field, Subfield

class MockArgs(object):
    pass

def get_mock_args():
    args = MockArgs()
    args.modified_after = None
    args.created_after = None
    args.mode = "write"
    args.max_number = 10
    args.format = "alephseq"
    args.authority_files = "tests/modified_authors.seq"
    args.resource_files = "tests/titles.seq"
    args.output_directory = "tests/temp_dir"
    args.identifier = "ID"
    args.identity_types = None

    return args

def get_isnis(record):
    isnis = []
    for field in record.get_fields('024'):
        if field['2']:
            if field['2'] == 'isni':
                isnis.append(field['a'])
    return isnis

def is_marked(record):
    for field in record.get_fields('924'):
        for sf in field.get_subfields('x'):
            if sf == "KESKEN-ISNI":
                return True
    return False

def is_sparse(record):
    for field in record.get_fields('924'):
        if field['x'] and field['q']:
            if field['x'] == "KESKEN-ISNI" and field['q'] == 'sparse record':
                return True
    return False

def number_of_fields(record, tag, code, content):
    number_of_fields = 0
    for field in record.get_fields(tag):
        for sf in field.get_subfields(code):
            if sf == content:
                number_of_fields += 1
    return number_of_fields

class MARC21ConverterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):   
         
        cls.authority_files = "tests/modified_authors.seq"
        cls.resource_files = "tests/titles.seq"
        cls.output_directory = "tests/temp_dir"
        sys.argv = sys.argv[:1]
        sys.argv.extend([
            "--mode", "write",
            "--format", "alephseq",
            "--authority_files", cls.authority_files,
            "--resource_files", cls.resource_files,
            "--output_directory", cls.output_directory,
            "--identifier", "ID",
            "--config_file_path", "tests/config.ini"
        ])
        c = Converter()
        c.config.read('tests/config.ini')
        cls.mc = c.converter
        cls.rl = ResourceList()

        return super(MARC21ConverterTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.output_directory)

    def test_get_modified(self):
        test_record_ids = ["000000001", "000000002", "000000003", "000000004"]
        expected_ids = ["000000003"]
        args = get_mock_args()
        args.modified_after = "2021-07-04"
        args.until = "2021-07-06"
        identities = self.mc.get_authority_data(args)
        tested_ids = []
        for id in test_record_ids:
            if id in identities:
                tested_ids.append(id)
        self.assertEqual(sorted(expected_ids), sorted(tested_ids))

    def test_get_dates(self):
        identity_type = 'personOrFiction'

        field = Field(tag = '046', subfields=[Subfield(code='f', value='-1900'),
                                              Subfield(code='g', value='2000')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['birthDate'], '-1900')
        self.assertEqual(dates['deathDate'], '2000')
        self.assertEqual(dates['dateType'], None)

        field = Field(tag = '046', subfields=[Subfield(code='s', value='900'),
                                              Subfield(code='t', value='2000')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['birthDate'], None)
        self.assertEqual(dates['deathDate'], '2000')
        self.assertEqual(dates['dateType'], 'flourished')

        field = Field(tag = '046', subfields=[Subfield(code='f', value='1900~')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['birthDate'], '1900')
        self.assertEqual(dates['dateType'], 'circa')

        field = Field(tag = '046', subfields=[Subfield(code='f', value='[1901,1902]'),
                                              Subfield(code='g', value='[1903,1905')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['birthDate'], '1901')
        self.assertEqual(dates['deathDate'], None)
        self.assertEqual(dates['dateType'], 'circa')

        identity_type = 'organisation'

        field = Field(tag = '046', subfields=[Subfield(code='f', value='-1900'),
                                              Subfield(code='g', value='2000')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['usageDateFrom'], None)
        self.assertEqual(dates['usageDateTo'], None)

        field = Field(tag = '046', subfields=[Subfield(code='s', value='-1900'),
                                              Subfield(code='t', value='2000')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['usageDateFrom'], '-1900')
        self.assertEqual(dates['usageDateTo'], '2000')

        field = Field(tag = '046', subfields=[Subfield(code='q', value='-1900'),
                                              Subfield(code='r', value='2000')])
        dates = self.mc.get_dates(field, identity_type)
        self.assertEqual(dates['usageDateFrom'], '-1900')
        self.assertEqual(dates['usageDateTo'], '2000')

    def test_create_isni_fields(self):
        args = get_mock_args()
        identities = self.mc.get_authority_data(args)
        isni_response = {'000000015': {'isni': '0000000474363482', 'sources': []},
                         '000000016': {'isni': '0000000484862619', 'sources': []},
                         '000000017': {'isni': '0000000000000001', 'sources': []},
                         '000000018': {'possible matches': {'000000001': {'source ids': ['000000018']}}},
                         '000000019': {'possible matches': {'000000001': {'source ids': ['000000018']}}}
                        }
        isni_records = self.mc.create_isni_fields(isni_response, args.identifier)

        self.assertNotIn('000000014', identities.keys())
        for record in isni_records:
            if record['001'].data == '000000015':
                self.assertIn('0000000474363482', get_isnis(record))
                self.assertEqual(len(get_isnis(record)), 1)
                self.assertEqual(is_marked(record), False)
            if record['001'].data == '000000016':
                self.assertIn('0000000484862619', get_isnis(record))
                self.assertEqual(len(get_isnis(record)), 1)
                self.assertEqual(is_marked(record), False)
            if record['001'].data == '000000017':
                self.assertIn('0000000000000001', get_isnis(record))
                self.assertEqual(len(get_isnis(record)), 1)
                self.assertEqual(number_of_fields(record, '924', 'q', 'isNot'), 2)
                self.assertEqual(is_marked(record), False)
            if record['001'].data == '000000018':
                self.assertEqual(get_isnis(record), [])
                self.assertEqual(is_marked(record), True)
                self.assertEqual(is_sparse(record), True)
            if record['001'].data == '000000019':
                self.assertEqual(number_of_fields(record, '924', 'q', '=Asterin tietue(et): 000000018'), 1)
                self.assertEqual(number_of_fields(record, '924', 'q', 'isNot'), 1)
                self.assertEqual(get_isnis(record), [])
                self.assertEqual(is_marked(record), True)
        isni_response = {'000000018': {'possible matches': {'000000001': {'source ids': ['000000017']}}}}

    def test_isni_instructions(self):
        identities = self.mc.get_authority_data(get_mock_args())
        self.assertIn({'identifier': '0000000492956869', 'type': 'ISNI'}, identities['000000020']['isNot'])
        self.assertIn({'identifier': '0000000474394188', 'type': 'ISNI'}, identities['000000020']['isNot'])
        self.assertIn({'identifier': '474394188', 'type': 'PPN'}, identities['000000020']['isNot'])
        self.assertEqual([], identities['000000020']['otherIdentifierOfIdentity'])
        self.assertIn({'identifier': '0000000474394188', 'type': 'ISNI'}, identities['000000021']['otherIdentifierOfIdentity'])
        self.assertEqual([], identities['000000021']['isNot'])
        self.assertNotEqual(0, len(identities['000000022']['errors']))
        self.assertIn({'identifier': '0000000474394188', 'type': 'ISNI'}, identities['000000023']['otherIdentifierOfIdentity'])
        self.assertNotIn('000000024', list(identities.keys()))

if __name__ == "__main__":
    unittest.main()
