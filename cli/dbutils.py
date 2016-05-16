import sqlite3
import os


connection = None


def create_and_connect(db_path):
    global connection
    connection = sqlite3.connect(db_path) 


def delete_database(db_path):
    os.remove(db_path)


def table_exists(table_name):
    c = connection.cursor()
    
    c.execute("SELECT * FROM sqlite_master WHERE name=? and type='table'", (table_name,))
    results = c.fetchone()
    
    c.close()
    
    return results != None


def column_exists(table_name, column_name):
    c = connection.cursor()
    
    c.execute("PRAGMA table_info('{}')".format(table_name))
    results = c.fetchall()
    
    for result in results:
        if result[1] == column_name:
            return True
    
    c.close()
    
    return False


def create_table(table_name, mappings=[]):
    if table_exists(table_name):
        return False
    
    columns = []
    key_columns = []
    
    for mapping in mappings:
        data_type =  mapping['data_type']
        column_name = mapping['column_name']
        
        if 'key' in mapping:
            if mapping['key'] == 'fk':
                # Create "foreign key table"
                create_table(column_name, [{
                    'column_name': 'value',
                    'data_type': data_type
                }])
                column_name = column_name + '_id'
            else:
                key_columns.append(column_name)
            
        columns.append('{} {}'.format(column_name, data_type))
        
    if len(key_columns) == 0:
        key_columns.append('id')
        columns.insert(0, 'id INTEGER')
    
    columns_str = ', '.join(columns)
    key_columns_str = ', '.join(key_columns)
    
    query = 'CREATE TABLE {} ({}, PRIMARY KEY ({}))'.format(
        table_name, columns_str, key_columns_str)
    
    c = connection.cursor()
    c.execute(query)
    c.close()
    
    return True
    

def insert(table_name, records):
    formatted_keys = ', '.join(["'{}'".format(val) for val in records.keys()])
    formatted_values = ', '.join(["'{}'".format(val) for val in records.values()])
    
    query = 'INSERT INTO {} ({}) VALUES ({})'.format(
        table_name, formatted_keys, formatted_values)
    
    c = connection.cursor()
    
    try:
        c.execute(query)
        connection.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    
    c.close()
    
    return success


def count(table_name):
    c = connection.cursor()
    
    c.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    results = c.fetchone()
    
    c.close()
    
    return results[0]


def select_all(table_name, columns):
    c = connection.cursor()
    
    joined_columns = ', '.join(columns)
    
    c.execute('SELECT {} FROM {}'.format(joined_columns, table_name))
    results = c.fetchall()
    
    c.close()
    
    the_list = []
    for result in results:
        r = {}
        
        i = 0
        for column in columns:
            r[column] = result[i]
            i += 1
    
        the_list.append(r)
        
    return the_list
