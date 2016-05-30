import libcsv2sqlite
import dbutils
import unittest
import transformations


class MappingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test1.csv',
            'mapping': 'test/test1.json',
            'output': 'tmp/test1.sqlite3',
        }

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_load_mapping_config(self):
        table_name, _, mappings = libcsv2sqlite.load_mapping_config(self.args['mapping'])

        self.assertEqual(len(mappings), 5)
        self.assertEqual(table_name, 'person')

    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(self.args)

        self.assertTrue(dbutils.table_exists('person'))
        self.assertEqual(dbutils.count('person'), 2)


class MappingTestForeignKey(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test2.csv',
            'mapping': 'test/test2.json',
            'output': 'tmp/test2.sqlite3',
        }

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(self.args)

        self.assertTrue(dbutils.table_exists('person'))
        self.assertEqual(dbutils.count('person'), 4)


class MappingTestPrimaryKey(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test3.csv',
            'mapping': 'test/test3.json',
            'output': 'tmp/test3.sqlite3',
        }

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_csv_to_sqlite3(self):
        libcsv2sqlite.csv_to_sqlite3(self.args)

        # If there was no pk, there should be 3 results
        self.assertEqual(dbutils.count('taxi'), 2)


class MappingTestFirstLine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test4.csv',
            'mapping': 'test/test4.json',
            'output': 'tmp/test4.sqlite3',
            'csv_has_title_columns': True,
        }

        libcsv2sqlite.csv_to_sqlite3(cls.args)

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_csv_to_sqlite3(self):
        self.assertEqual(dbutils.count('taxi'), 2)

    def test_column_names(self):
        self.assertTrue(dbutils.column_exists('taxi', 'HappyEmail'))
        self.assertTrue(dbutils.column_exists('taxi', 'phone'))


class CustomTransformationsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test5.csv',
            'mapping': 'test/test5.json',
            'output': 'tmp/test5.sqlite3',
            'csv_has_title_columns': True,
        }

        libcsv2sqlite.csv_to_sqlite3(cls.args)

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_explode_existence(self):
        self.assertTrue(hasattr(transformations, 'explode'))


class WeirdHeadersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test6.csv',
            'mapping': 'test/test6.json',
            'output': 'tmp/test6.sqlite3',
            'csv_has_title_columns': True,
        }

        libcsv2sqlite.csv_to_sqlite3(cls.args)

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_weird_headers(self):
        # Happy $8124 0*$ Email
        self.assertTrue(dbutils.column_exists('taxi', 'Happy_8124_0_Email'))

        # )()(Happy   @@ Ph@one $$
        self.assertTrue(dbutils.column_exists('taxi', 'phone'))


class AnonymousColumnsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test8.csv',
            'mapping': 'test/test8.json',
            'output': 'tmp/test8.sqlite3',
            'csv_has_title_columns': False,
        }

        libcsv2sqlite.csv_to_sqlite3(cls.args)

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_generated_column_names(self):
        self.assertTrue(dbutils.column_exists('email', 'column_0'))
        self.assertTrue(dbutils.column_exists('email', 'column_1'))


class MissingColumnTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'input': 'test/test9.csv',
            'mapping': 'test/test9.json',
            'output': 'tmp/test9.sqlite3',
            'csv_has_title_columns': True
        }

        libcsv2sqlite.csv_to_sqlite3(cls.args)

    @classmethod
    def tearDownClass(cls):
        dbutils.connection.close()
        dbutils.delete_database(cls.args['output'])

    def test_generated_column_names(self):
        self.assertTrue(dbutils.column_exists('person', 'pretty_name'))
        self.assertTrue(dbutils.column_exists('person', 'Age'))
        self.assertFalse(dbutils.column_exists('person', 'Gender'))


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

        exists = dbutils.table_exists(table_name)
        self.assertFalse(exists)

        exists = dbutils.create_table(table_name)
        self.assertTrue(exists)

        exists = dbutils.table_exists(table_name)
        self.assertTrue(exists)

    def test_select_all(self):
        table_name = 'hmmm3'

        dbutils.create_table(table_name, [{
            'column_name': 'yeah',
            'data_type': 'VARCHAR(-1)'
        }])

        dbutils.insert(table_name, {'yeah': 'bla'})
        dbutils.insert(table_name, {'yeah': 'ble'})

        all_results = dbutils.select_all(table_name, ['id', 'yeah'])

        self.assertEqual(all_results[0]['yeah'], 'bla')


class TransformationsTest(unittest.TestCase):
    def test_transformations(self):
        self.assertEqual(transformations.sqlite_upper('aaa'), 'AAA')
        self.assertEqual(transformations.sqlite_upper('111'), '111')
        self.assertEqual(transformations.sqlite_upper('eAq'), 'EAQ')

        self.assertEqual(transformations.sqlite_lower('AAA'), 'aaa')
        self.assertEqual(transformations.sqlite_lower('111'), '111')
        self.assertEqual(transformations.sqlite_lower('EAQ'), 'eaq')

        self.assertEqual(transformations.sqlite_abs(123), 123)
        self.assertEqual(transformations.sqlite_abs(-123), 123)
        self.assertEqual(transformations.sqlite_abs(0), 0)

        self.assertEqual(transformations.sqlite_length('EAQ'), 3)
        self.assertEqual(transformations.sqlite_length(''), 0)

        self.assertEqual(transformations.sqlite_ltrim('     EAQ'), 'EAQ')
        self.assertEqual(transformations.sqlite_ltrim('\tEAQ'), 'EAQ')
        self.assertEqual(transformations.sqlite_ltrim('    EAQ   '), 'EAQ   ')

        self.assertEqual(transformations.sqlite_rtrim('EAQ    '), 'EAQ')
        self.assertEqual(transformations.sqlite_rtrim('EAQ\t'), 'EAQ')
        self.assertEqual(transformations.sqlite_rtrim('    EAQ   '), '    EAQ')

        self.assertEqual(transformations.sqlite_trim('EAQ    '), 'EAQ')
        self.assertEqual(transformations.sqlite_trim('EAQ\t'), 'EAQ')
        self.assertEqual(transformations.sqlite_trim('    EAQ   '), 'EAQ')

        self.assertTrue(isinstance(transformations.sqlite_random(), int))

        self.assertEqual(transformations.sqlite_round(4), 4)
        self.assertEqual(transformations.sqlite_round(4.6), 5)
        self.assertEqual(transformations.sqlite_round(4.2), 4)
        self.assertEqual(transformations.sqlite_round(0), 0)

        self.assertEqual(transformations.sqlite_typeof(None), 'null')
        self.assertEqual(transformations.sqlite_typeof(1), 'integer')
        self.assertEqual(transformations.sqlite_typeof(2.34), 'real')
        self.assertEqual(transformations.sqlite_typeof('asdasdasd'), 'text')


class DataTypeGuesserTest(unittest.TestCase):
    def test_get_data_type(self):
        self.assertEqual(libcsv2sqlite.get_data_type('2'), int)
        self.assertEqual(libcsv2sqlite.get_data_type('potato'), str)
        self.assertEqual(libcsv2sqlite.get_data_type('2.1'), float)
        self.assertEqual(libcsv2sqlite.get_data_type('0.0'), float)

    def test_guess_column_type(self):
        def create_column_gen(mylist):
            for val in mylist:
                yield val

        self.assertEqual(libcsv2sqlite.guess_column_type(create_column_gen(
            ['2', '3', '5', '-1', '0', '1231231231', '-23123121'])), int)

        self.assertEqual(libcsv2sqlite.guess_column_type(create_column_gen(
            ['2', '3', '5', '-1', '0.4', '1231231231', '-23123121'])), float)

        self.assertEqual(libcsv2sqlite.guess_column_type(create_column_gen(
            [' 2', '3 ', '\t5 ', '-1', '0.4', '-12.0', '-23123121'])), float)

        self.assertEqual(libcsv2sqlite.guess_column_type(create_column_gen(
            ['2', '3', '5', '-1', '0.4qwdqwdqwd', '1231231231', '-23123121'])), str)


class DuplicateMappingColumnNameTest(unittest.TestCase):
    def test_duplicates(self):
        mappings = [
            {"column_name": "Name"},
            {"column_name": "Name_1"},
            {"column_name": "Name"},
            {"column_name": "Animal"},
            {"column_name": "Animal"}
        ]

        libcsv2sqlite.uniquefy_names(mappings)

        self.assertEqual(mappings[0]['column_name'], 'Name')
        self.assertEqual(mappings[1]['column_name'], 'Name_1')
        self.assertEqual(mappings[2]['column_name'], 'Name_2')
        self.assertEqual(mappings[3]['column_name'], 'Animal')
        self.assertEqual(mappings[4]['column_name'], 'Animal_1')


unittest.main()
