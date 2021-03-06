import sqlite3
import os


connection = None


def python_to_sqlite_type(python_type):
    sqlite_types = {
        int: 'INTEGER',
        float: 'REAL',
        str: 'TEXT'
    }

    return sqlite_types[python_type]


def create_and_connect(db_path):
    global connection
    connection = sqlite3.connect(db_path)


def delete_database(db_path):
    os.remove(db_path)


def table_exists(table_name):
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM sqlite_master WHERE name=? and type='table'", (table_name,))
    results = cursor.fetchone()

    cursor.close()

    return results != None


def column_exists(table_name, column_name):
    cursor = connection.cursor()

    cursor.execute("PRAGMA table_info('{}')".format(table_name))
    results = cursor.fetchall()

    for result in results:
        if result[1] == column_name:
            return True

    cursor.close()

    return False


def column_is_pk(table_name, column_name):
    cursor = connection.cursor()

    cursor.execute("PRAGMA table_info('{}')".format(table_name))
    results = cursor.fetchall()

    for result in results:
        if result[1] == column_name:
            return True if result[5] == 1 else False

    cursor.close()

    return False

def create_table(table_name, mappings=[]):
    if table_exists(table_name):
        return False

    all_columns = []
    pk_key_columns = []
    fk_key_columns = []

    for mapping in mappings:
        data_type = mapping['data_type']
        column_name = mapping['column_name']

        if 'key' in mapping:
            if mapping['key'] == 'fk':
                # Create "foreign key table"
                create_table(column_name, [{
                    'column_name': 'value',
                    'data_type': data_type
                }])
                fk_key_columns.append(column_name)
                column_name = column_name + '_id'
            else:
                pk_key_columns.append(column_name)

        all_columns.append('{} {}'.format(column_name, data_type))

    if len(pk_key_columns) == 0:
        pk_key_columns.append('id')
        all_columns.insert(0, 'id INTEGER')

    if len(fk_key_columns) > 0:
        for i, col in enumerate(fk_key_columns):
            fk_key_columns[i] = 'FOREIGN KEY ({}_id) REFERENCES {}(id)'.format(col, col)

    all_columns_str = ', '.join(all_columns)
    pk_key_columns_str = ', '.join(pk_key_columns)
    fk_key_columns_str = ', '.join(fk_key_columns)

    if len(fk_key_columns_str) > 0:
        fk_key_columns_str = ', ' + fk_key_columns_str

    query = 'CREATE TABLE {} ({}, PRIMARY KEY ({}) {})'.format(
        table_name, all_columns_str, pk_key_columns_str, fk_key_columns_str)

    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

    return True


def insert(table_name, records):
    formatted_keys = ', '.join(["'{}'".format(val) for val in records.keys()])
    formatted_values = ', '.join(["'{}'".format(val) for val in records.values()])

    query = 'INSERT INTO {} ({}) VALUES ({})'.format(
        table_name, formatted_keys, formatted_values)

    cursor = connection.cursor()

    try:
        cursor.execute(query)
        connection.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False

    cursor.close()

    return success


def insert_many(table_name, keys, records):
    if not len(records):
        return True

    formatted_keys = ', '.join(["'{}'".format(key) for key in keys])
    question_marks = ', '.join(['?' for i in range(0, len(keys))])

    query = 'INSERT INTO {} ({}) VALUES ({})'.format(
        table_name, formatted_keys, question_marks)

    cursor = connection.cursor()

    try:
        cursor.executemany(query, records)
        connection.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False

    cursor.close()

    return success


def count(table_name):
    cursor = connection.cursor()

    cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    results = cursor.fetchone()

    cursor.close()

    return results[0]

def select_all(table_name, columns=None):
    cursor = connection.cursor()

    if columns:
        joined_columns = ', '.join(columns)
        cursor.execute('SELECT {} FROM {}'.format(joined_columns, table_name))
    else:
        cursor.execute('SELECT * FROM {}'.format(table_name))

    results = cursor.fetchall()
    cursor.close()

    if not columns:
        columns = []
        for i in range(0, len(results[0])):
            columns.append('column' + str(i))

    result_list = []
    for result in results:
        result_dict = {}

        i = 0
        for column in columns:
            result_dict[column] = result[i]
            i += 1

        result_list.append(result_dict)

    return result_list
