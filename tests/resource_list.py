import unittest
from resource_list import ResourceList
from pymarc import Record, Field

class ResourceListTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        input_file = "tests/titles.mrc"
        format = "marc21"
        cls.rl = ResourceList(input_file, format)

    def test_number_of_titles(self):
        numbers = {'000000001': 2,
                   '000000002': 3,
                   '000000003': 1,
                   '000000004': 1
                   }
        for id in self.rl.titles:
            self.assertEqual(len(self.rl.titles[id]), numbers[id])

    def test_title_data(self):
        titles = self.rl.titles['000000004']
        for title in titles:
            self.assertEqual(title['title'], 'Nimi2 Tarkenne3')
            self.assertEqual(title['creationClass'], 'om')
            self.assertEqual(title['publisher'], 'Kustantaja')
            self.assertEqual(title['date'], '1990')
            self.assertEqual(title['language'], 'fin')
            self.assertEqual(title['creationRole'], 'aut')
            self.assertEqual(title['role'], 'author')

    def test_get_identifiers(self):
        record = Record()
        record.add_field(Field(
            tag = '020',
            indicators = [' ',' '],
            subfields = [
                'a', '9781234567897',
        ]))
        record.add_field(Field(
            tag = '022',
            indicators = [' ',' '],
            subfields = [
                'a', '1455-8904',
        ]))
        record.add_field(Field(
            tag = '024',
            indicators = ['0',' '],
            subfields = [
                'a', 'NLC018413261',
        ]))
        record.add_field(Field(
            tag = '024',
            indicators = ['2',' '],
            subfields = [
                'a', 'M-321-76543-1',
        ]))
        record.add_field(Field(
            tag = '024',
            indicators = ['7',''],
            subfields = [
                'a', '10.1228/0103000001002',
                '2', 'doi'
        ]))

        identifiers = self.rl.get_identifiers(record)
        for id_type in identifiers:
            if id_type == 'ISBN':
                self.assertEqual(sorted(['9781234567897']), sorted(identifiers[id_type]))
            elif id_type == 'ISSN':
                self.assertEqual(sorted(['14558904']), sorted(identifiers[id_type]))
            elif id_type == 'ISRC':
                self.assertEqual(sorted(['NLC018413261']), sorted(identifiers[id_type]))
            elif id_type == 'ISMN':
                self.assertEqual(sorted(['M321765431']), sorted(identifiers[id_type]))
            elif id_type == 'DOI':
                self.assertEqual(sorted(['10.1228/0103000001002']), sorted(identifiers[id_type]))

if __name__ == "__main__":
    unittest.main()