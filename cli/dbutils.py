import sqlite3
import os


connection = None


def create_and_connect(db_path):
    global connection
    connection = sqlite3.connect(db_path) 


def delete_database(db_path):
    os.remove(db_path)


def table_does_exist(table_name):
    c = connection.cursor()
    
    c.execute("SELECT * FROM sqlite_master WHERE name=? and type='table'", (table_name,))
    results = c.fetchone()
    
    c.close()
    
    return results != None


def create_table(table_name):
    if not table_does_exist(table_name):
        c = connection.cursor()
        c.execute('CREATE TABLE %s ( ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL )' % (table_name,))
        c.close()
        return True
        
    return False

# TODO: SQLite does not allow alter column statements to add primary keys
def add_column(table_name, column_name, db_data_type='VARCHAR(-1)', pk=False):
    c = connection.cursor()
    
    # Try creating. If it already exists, it'll just pass
    create_table(table_name)
    
    try:
    
        if column_name == 'email':
            import pdb; pdb.set_trace()
    
        c.execute('ALTER TABLE %s ADD COLUMN %s %s %s' %(
            table_name,
            column_name,
            db_data_type,
            'PRIMARY KEY' if pk else ''))
            
        success = True
    except Exception as detail:
        success = False
    
    c.close()
    
    return success


def insert(table_name, values):
    c = connection.cursor()
    
    query = 'INSERT INTO %s (' % (table_name,)
    
    for k in values.keys():
        query += '%s,' % (k,) 
        
    # Remove last comma
    query = query[:-1]
    
    query += ') VALUES ('
    
    for v in values.values():
        query += "'%s'," % (v,)
        
    # Remove last comma
    query = query[:-1]
    
    query += ')'   
    
    c.execute(query)
    connection.commit()
    
    c.close()


def count(table_name):
    c = connection.cursor()
    
    c.execute('SELECT COUNT(*) FROM %s' % (table_name,))
    results = c.fetchone()
    
    c.close()
    
    return results[0]


def select_all(table_name, columns):
    c = connection.cursor()
    
    joined_columns = ', '.join(columns)
    
    c.execute('SELECT %s FROM %s' % (joined_columns, table_name,))
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
