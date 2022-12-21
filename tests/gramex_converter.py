import unittest
from gramex_converter import GramexConverter

class MockArgs(object):
    pass

class GramexConverterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):   
         
        cls.authority_files = "tests/authors.csv"
        cls.resource_files = "tests/tracks.csv"
        args = MockArgs()
        args.authority_files = cls.authority_files
        args.resource_files = cls.resource_files
        mc = GramexConverter()
        cls.results = mc.get_authority_data(args)
        
        return super(GramexConverterTest, cls).setUpClass()
    
    def test_number_of_performers_with_records(self):
        resource_count = 0
        for id in self.results:
            if self.results[id]['resource']:
                resource_count += 1
        self.assertEqual(resource_count, 8)

    def test_matching_records_with_name(self):
        self.assertEqual(len(self.results['7']['resource']), 1)
        self.assertEqual(len(self.results['7(00016)']['resource']), 0)

    def test_person_affiliations(self):
        relation = self.results['1']['isRelated'][0]
        self.assertEqual(relation['identityType'], 'organisation')
        self.assertEqual(relation['relationType'], 'isMemberOf')
        self.assertEqual(relation['organisationName']['mainName'], 'MEIKÄLÄISET')

    def test_person_living_years(self):
        self.assertEqual(self.results['1']['birthDate'], '1901-01-01')
        self.assertEqual(self.results['1']['deathDate'], '2000-12-31')
    
    def test_resource_sorting(self):
        resources = self.results['1']['resource']
        self.assertEqual(resources[0]['title'], 'KAPPALE 6')
        self.assertEqual(resources[1]['title'], 'KAPPALE 1')
        self.assertEqual(resources[-1]['title'], 'KAPPALE 2')

    def test_pseudonym_resources(self):
        self.assertEqual(len(self.results['2']['resource']), 1)
        self.assertEqual(self.results['2']['resource'][0]['title'], 'KAPPALE 9')
        self.assertEqual(len(self.results['2(00004)']['resource']), 1)
        self.assertEqual(self.results['2(00004)']['resource'][0]['title'], 'KAPPALE 8')
        self.assertEqual(len(self.results['4']['resource']), 1)
        self.assertEqual(self.results['4']['resource'][0]['title'], 'KAPPALE 14')
        self.assertEqual(len(self.results['4(00009)']['resource']), 1)
        self.assertEqual(self.results['4(00009)']['resource'][0]['title'], 'KAPPALE 15')
        self.assertEqual(len(self.results['4(00010)']['resource']), 1)
        self.assertEqual(self.results['4(00010)']['resource'][0]['title'], 'KAPPALE 12')

    def test_person_pseudonyms(self):
        relations = self.results['4']['isRelated']
        self.assertEqual(len(relations), 2)
        pseudonym = relations[1]
        self.assertEqual(pseudonym['identityType'], 'personOrFiction')
        self.assertEqual(pseudonym['personalName']['forename'], 'TOINEN')
        self.assertEqual(pseudonym['personalName']['surname'], 'PSEUDONYYMI')

    def test_real_identity(self):
        relations = self.results['4(00010)']['isRelated']
        self.assertEqual(len(relations), 1)
        real_name = None
        for relation in relations:
            if relation['relationType'] == 'real name':
                real_name = relation
        self.assertEqual(real_name['identityType'], 'personOrFiction')
        self.assertEqual(real_name['personalName']['surname'], 'ESIINTYJÄNIMI')

    def test_variant_of_artist_name_exists(self):
        variant_name = self.results['8']['personalNameVariant'][0]
        self.assertEqual(variant_name['forename'], 'NIMI')
        self.assertEqual(variant_name['surname'], 'VARIANTTI')

if __name__ == "__main__":
    unittest.main()