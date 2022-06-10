import unittest
import configparser
import json
import os
import shutil
import sys
from unittest.mock import Mock
from marc21_converter import MARC21Converter
from converter import Converter
from resource_list import ResourceList
from tools import aleph_seq_reader 

class MockArgs(object):
    pass

class MARC21ConverterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):   
         
        cls.authority_files = "tests/modified_authors.seq"
        cls.resource_files = "tests/titles.seq"
        cls.output_marc_fields = "tests/temp.seq"
        cls.output_directory = "tests/temp_dir"
        sys.argv = sys.argv[:1]
        sys.argv.extend([
            "--mode", "write",
            "--max_number", "10",
            "--format", "alephseq",
            "--authority_files", cls.authority_files,
            "--resource_files", cls.resource_files,
            "--output_directory", cls.output_directory,
            "--identifier", "ID"
        ])    
        cls.c = Converter()
        cls.mc = MARC21Converter()
        cls.rl = ResourceList()
        cls.config = configparser.ConfigParser()
        cls.config.read('tests/config.ini')
        cls.mc.config.read('tests/config.ini')
        cls.mc.cataloguers = json.loads(cls.config['SETTINGS'].get('cataloguers'))
        
        return super(MARC21ConverterTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.output_marc_fields)
        shutil.rmtree(cls.output_directory)

    def test_get_modified(self):
        test_record_ids = ["000000001", "000000002", "000000003", "000000004"]
        expected_ids = ["000000003"]
        args = MockArgs()
        args.modified_after = "2021-07-04"
        args.created_after = None
        args.until = "2021-07-06"
        args.mode = "write"
        args.max_number = 10
        args.format = "alephseq"
        args.authority_files = self.authority_files
        args.resource_files = self.resource_files
        args.output_directory = self.output_directory
        args.identifier = "ID"
        args.identity_types = None
        identities = self.mc.get_authority_data(args)
        tested_ids = []
        for id in test_record_ids:
            if id in identities:
                tested_ids.append(id)
        self.assertEqual(sorted(expected_ids), sorted(tested_ids))

if __name__ == "__main__":
    unittest.main()
