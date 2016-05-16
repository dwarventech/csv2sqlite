import os
import csv
import json
import collections
import importlib.util

import dbutils
import transformations


def load_mapping_config(mapping_path):
    with open(mapping_path) as data_file:    
        j = json.load(data_file)
        
        t = j['transformations'] if 'transformations' in j else None
        return (j['table_name'], t, j['mappings'])


def get_mappings_by_csv_index(mappings, index):
    the_mappings = []

    for mapping in mappings:
        if mapping['csv_index'] == index:
            the_mappings.append(mapping)

    return the_mappings


def import_csv_row(row, table_name, mappings):
    length = len(row)
    
    values = {}
    
    for i in range(0, length):
        index_mappings = get_mappings_by_csv_index(mappings, i)
        
        the_value = row[i]
        
        for mapping in index_mappings:
            column_name = mapping['column_name']
        
            if 'transform' in mapping:
                func_name = mapping['transform']
                func = getattr(transformations, func_name)
                the_value = func(the_value)
               
            if 'key' in mapping and mapping['key'] == 'fk':
                column_name = column_name + '_id'
            
            values[column_name] = the_value

    dbutils.insert(table_name, values)


def read_key_mappings(all_data, mappings):
    fk_mappings = []
    pk_mapping = None
    
    for mapping in mappings:
        column_name = mapping['column_name']
        
        if 'key' in mapping and mapping['key'] == 'fk':
            fk_mappings.append(mapping)
            
        elif 'key' in mapping and mapping['key'] == 'pk':
            pk_mapping = mapping
            
    for mapping in fk_mappings:
        mapping['dataset'] = set()

    for row in all_data:
        for mapping in fk_mappings:
            index = mapping['csv_index']
            mapping['dataset'].add(row[index])
    
    return fk_mappings, pk_mapping


def fk_mappings_to_database(fk_mappings):
    if not fk_mappings:
        return
        
    fk_patch_data = []
    
    for mapping in fk_mappings:
        # FK tables have the same name as FK column
        table_name = column_name = mapping['column_name']
        
        db_results = dbutils.select_all(column_name, ['id', 'value'])
        
        db_set = set()
        for result in db_results:
            db_set.add(result['value'])
        
        diff_set = mapping['dataset'] - db_set
        
        # Insert only new values in the database
        for v in diff_set:
            dbutils.insert(table_name, { 'value': v })
        
        fk_patch_data.append({
            'csv_index': mapping['csv_index'],
            'db_values': dbutils.select_all(table_name, ['id', 'value']),
        })
        
    return fk_patch_data


def pk_table_to_database(pk_table):
    if not pk_table:
        return
        
    # dbutils.select_all()

def get_column_id(index, column, values):
    for v in values:
        if column == v['value']:
            return v['id']
    
        
def patch_csv_data(fk_patch_data, all_csv_data):
    if not fk_patch_data:
        return

    for row in all_csv_data:
        for fk_patch_item in fk_patch_data:
            index = fk_patch_item['csv_index']
            
            # Replace row value with respective id from new table
            row[index] = get_column_id(index, row[index], fk_patch_item['db_values'])


def set_mapping_defaults(mappings, headers):
    for i, mapping in enumerate(mappings):
        if 'data_type' not in mapping:
            mapping['data_type'] = 'VARCHAR(-1)'
        
        if 'column_name' not in mapping and len(headers) > 0:
            mapping['column_name'] = headers[i]
    

def load_custom_transformations(mapping_path, custom_transformations_path):
    # use json path as reference
    path = os.path.abspath(os.path.dirname(mapping_path))

    # remove extension
    module_name = os.path.splitext(custom_transformations_path)[0]
    
    # custom loading stuff
    spec = importlib.util.spec_from_file_location(
        module_name, os.path.join(path, custom_transformations_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    for function_name in dir(module):
        # Remove internal stuff
        if function_name[0:2] != '__':
            func = getattr(module, function_name)
            setattr(transformations, function_name, func)


def csv_to_sqlite3(csv_path, mapping_path, db_path, csv_has_title_columns=False):
    # Load csv file into a list
    all_csv_data = []
    with open(csv_path) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            all_csv_data.append(row)

    headers = []
    if csv_has_title_columns:
        headers = all_csv_data[0]
        all_csv_data = all_csv_data[1:]

    dbutils.create_and_connect(db_path)
    
    # Load config
    table_name, custom_transformations, mappings = load_mapping_config(mapping_path)
    
    if custom_transformations:
        load_custom_transformations(mapping_path, custom_transformations)
    
    set_mapping_defaults(mappings, headers)

    # Create database table
    dbutils.create_table(table_name, mappings)

    # Load fk tables    
    fk_mappings, pk_mapping = read_key_mappings(all_csv_data, mappings)
    
    fk_patch_data = fk_mappings_to_database(fk_mappings)    
    
    # Substitute data with foreign key IDs
    patch_csv_data(fk_patch_data, all_csv_data)
    
    # Import result
    for row in all_csv_data:
        import_csv_row(row, table_name, mappings)
