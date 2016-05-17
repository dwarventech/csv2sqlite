import libcsv2sqlite
import dbutils
import unittest
import transformations

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
        table_name, transformations, mappings = libcsv2sqlite.load_mapping_config(self.mapping_path)
        
        self.assertEqual(len(mappings), 5)
        self.assertEqual(table_name, 'person')
    
    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(
            self.csv_path,
            self.mapping_path,
            self.db_path)
            
        self.assertTrue(dbutils.table_exists('person'))
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
            
        self.assertTrue(dbutils.table_exists('person'))
        self.assertEqual(dbutils.count('person'), 4)


class MappingTestPrimaryKey(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/test3.sqlite3'
        cls.mapping_path = 'test/test3_mapping.json'
        cls.csv_path = 'test/test3.csv'

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)

    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(
            self.csv_path,
            self.mapping_path,
            self.db_path)
        
        # If there was no pk, there should be 3 results
        self.assertEqual(dbutils.count('taxi'), 2)


class MappingTestFirstLine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/test4.sqlite3'
        cls.mapping_path = 'test/test4_mapping.json'
        cls.csv_path = 'test/test4.csv'
        cls.csv_has_title_columns = True
        
        libcsv2sqlite.csv_to_sqlite3(
            cls.csv_path,
            cls.mapping_path,
            cls.db_path,
            cls.csv_has_title_columns)
        
    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)

    def test_csv_to_sqlite3(self):
        self.assertEqual(dbutils.count('taxi'), 2)
    
    def test_column_names(self):
        self.assertTrue(dbutils.column_exists('taxi', 'HappyEmail'))
        self.assertTrue(dbutils.column_exists('taxi', 'phone'))


class CustomTransformationsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/test5.sqlite3'
        cls.mapping_path = 'test/test5_mapping.json'
        cls.csv_path = 'test/test5.csv'
        cls.csv_has_title_columns = True
        
        libcsv2sqlite.csv_to_sqlite3(
            cls.csv_path,
            cls.mapping_path,
            cls.db_path,
            cls.csv_has_title_columns)
        
    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)

    def test_explode_existence(self):
        self.assertTrue(hasattr(transformations, 'explode'))


class WeirdHeadersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tmp/test6.sqlite3'
        cls.mapping_path = 'test/test6_mapping.json'
        cls.csv_path = 'test/test6.csv'
        cls.csv_has_title_columns = True
        
        libcsv2sqlite.csv_to_sqlite3(
            cls.csv_path,
            cls.mapping_path,
            cls.db_path,
            cls.csv_has_title_columns)
        
    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.db_path)

    def test_explode_existence(self):
        # Happy $8124 0*$ Email
        self.assertTrue(dbutils.column_exists('taxi', 'Happy_8124_0_Email'))
        
        # )()(Happy   @@ Ph@one $$ 
        self.assertTrue(dbutils.column_exists('taxi', '_Happy_Ph_one_'))


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
        b = dbutils.table_exists(table_name)
        self.assertFalse(b)

        b = dbutils.create_table(table_name)
        self.assertTrue(b)
        
        b = dbutils.table_exists(table_name)
        self.assertTrue(b)
        
    def test_select_all(self):
        table_name = 'hmmm3'
        
        b = dbutils.create_table(table_name, [{
            'column_name': 'yeah',
            'data_type': 'VARCHAR(-1)'
        }])
        
        dbutils.insert(table_name, { 'yeah': 'bla' })
        dbutils.insert(table_name, { 'yeah': 'ble' })
        
        all = dbutils.select_all(table_name, ['id', 'yeah'])
        
        self.assertEqual(all[0]['yeah'], 'bla')
        

unittest.main()