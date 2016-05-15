import libcsv2sqlite
import dbutils
import unittest


class MappingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/test1.sqlite3'
        cls.mapping_path = 'test/test1_mapping.json'
        cls.csv_path = 'test/test1.csv'

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)

    def test_load_mapping_config(self):
        table_name, mappings = libcsv2sqlite.load_mapping_config(self.mapping_path)
        
        self.assertEqual(len(mappings), 5)
        self.assertEqual(table_name, 'person')
    
    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(
            self.csv_path,
            self.mapping_path,
            self.db_path)
            
        self.assertTrue(dbutils.table_does_exist('person'))
        
        # Adding a column should return false because it already exists
        self.assertFalse(dbutils.add_column('person', 'name'))
        
        self.assertEqual(dbutils.count('person'), 2)
        
        
class MappingTestForeignKey(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/test2.sqlite3'
        cls.mapping_path = 'test/test2_mapping.json'
        cls.csv_path = 'test/test2.csv'

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)

    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(
            self.csv_path,
            self.mapping_path,
            self.db_path)
            
        self.assertTrue(dbutils.table_does_exist('person'))
        
        # Adding a column should return false because it already exists
        self.assertFalse(dbutils.add_column('person', 'name'))
        
        self.assertEqual(dbutils.count('person'), 4)
        

class DbUtilsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/dbtest.sqlite3'
        dbutils.create_and_connect(cls.db_path)

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)
        
    def test_table_existence(self):
        table_name = 'bla'
        b = dbutils.table_does_exist(table_name)
        self.assertFalse(b)

        b = dbutils.create_table(table_name)
        self.assertTrue(b)
        
        b = dbutils.table_does_exist(table_name)
        self.assertTrue(b)
        
    def test_add_column(self):
        table_name = 'hmmm'
        b = dbutils.create_table(table_name)
        self.assertTrue(b)
        
        b = dbutils.add_column(table_name, 'yeah')
        self.assertTrue(b)
        
        # Try adding again
        b = dbutils.add_column(table_name, 'yeah')
        self.assertFalse(b)
        
    def test_add_column_and_table(self):
        table_name = 'hmmm2'
        
        b = dbutils.add_column(table_name, 'yeah')
        self.assertTrue(b)
        
        b = dbutils.table_does_exist(table_name)
        self.assertTrue(b)
    
    def test_select_all(self):
        table_name = 'hmmm3'
        
        b = dbutils.add_column(table_name, 'yeah')
        self.assertTrue(b)
        
        dbutils.insert(table_name, { 'yeah': 'bla' })
        dbutils.insert(table_name, { 'yeah': 'ble' })
        
        all = dbutils.select_all(table_name, ['id', 'yeah'])
        
        self.assertEqual(all[0]['yeah'], 'bla')
        

unittest.main()