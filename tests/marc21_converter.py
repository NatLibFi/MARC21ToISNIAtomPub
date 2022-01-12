import unittest
import configparser
import json
import os
import shutil
import sys
from unittest.mock import Mock
from marc21_converter import MARC21Converter
from converter import Converter
from pymarc import MARCReader, Record, Field
from tools import aleph_seq_reader 

class MockArgs(object):
    pass




class MARC21ConverterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):   
         
        cls.authority_files = "tests/authors.seq"
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
        cls.config = configparser.ConfigParser()
        cls.config.read('tests/config.ini')
        cls.mc.cataloguers = json.loads(cls.config['SETTINGS'].get('cataloguers'))
        cls.args = MockArgs()
        cls.args.output_marc_fields = cls.output_marc_fields

        cls.isnis = {'000000001': '0000000000000001',
                     '000000002': '0000000000000002',
                     '000000003': '0000000000000003',
                     '000000004': '0000000000000004'}
        cls.mc.records = cls.c.converter.records
        cls.mc.write_isni_fields(cls.isnis, cls.args)
        input_reader = aleph_seq_reader.AlephSeqReader(open(cls.authority_files, 'r', encoding="utf-8"))
        output_reader = aleph_seq_reader.AlephSeqReader(open(cls.output_marc_fields, 'r', encoding="utf-8"))
        cls.input_records = []
        cls.output_records = []
        for record in input_reader:
            cls.input_records.append(record)
        for record in output_reader:
            cls.output_records.append(record)    
        input_reader.close()
        output_reader.close()
        
        return super(MARC21ConverterTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.output_marc_fields)
        shutil.rmtree(cls.output_directory)

    def get_input_output_pairs(self):
        pairs = dict()
        for input_record in self.input_records:
            input_id = input_record['001'].data
            pairs[input_id] = {'input': input_record}
            isni = None
            if input_id in self.isnis:
                isni = self.isnis[input_id]
                if isni:
                    for output_record in self.output_records:
                        output_id = output_record['001'].data
                        if input_id == output_id:
                            pairs[input_id]['output'] = output_record
        return pairs

    def test_duplicate_output_fields(self):
        for output_record in self.output_records:
            for output_record in self.output_records:
                for field1 in output_record.get_fields('024'):
                    counter = 0
                    for field2 in output_record.get_fields('024'):
                        if field1.subfields == field2.subfields:
                            counter += 1
                    self.assertEqual(counter, 1)
    
    def test_isni_changed(self):
        test_record_ids = ["000000001", "000000002", "000000003"]
        tested_ids = []
        for output_record in self.output_records:
            id = output_record['001'].data
            if id in test_record_ids:
                for field in output_record.get_fields('024'):
                    for sf in field.get_subfields('2'):
                        if sf == 'isni':
                            self.assertEqual(field['a'], self.isnis[id])
                            tested_ids.append(id)
        self.assertEqual(sorted(test_record_ids), sorted(tested_ids))

    def test_original_data_unchanged_in_output_fields(self):
        test_record_ids = ["000000001"]
        tested_ids = []
        pairs = self.get_input_output_pairs()
        for id in pairs:
            if id in test_record_ids:
                for field1 in pairs[id]['input'].get_fields('024'):
                    counter = 0
                    if field1['2']:
                        if field1['2'] != 'isni':
                            for field2 in pairs[id]['output'].get_fields('024'):
                                if field1.subfields == field2.subfields:
                                    counter += 1
                            self.assertEqual(counter, 1)
                            tested_ids.append(id)
        self.assertEqual(sorted(test_record_ids), sorted(tested_ids))

    def test_already_catalogued_not_in_output_fields(self):
        test_record_ids = ["000000004"]
        tested_ids = []
        pairs = self.get_input_output_pairs()
        for id in test_record_ids:
            if id in pairs:
                tested_ids.append(id)
                self.assertNotIn('output', pairs[id])
        self.assertEqual(sorted(test_record_ids), sorted(tested_ids))
        
if __name__ == "__main__":
    unittest.main()
