import argparse
import libcsv2sqlite


parser = argparse.ArgumentParser(
    description='Import CSV files into new or existing SQLite databases.'
)

required_named = parser.add_argument_group('required arguments')

required_named.add_argument(
    '--input', '-i',
    metavar='data.csv',
    type=str,
    help='path of the source CSV file',
    required=True
)

parser.add_argument(
    '--output', '-o',
    metavar='db.sqlite',
    type=str,
    help='path of the destination database file',
    required=False,
    default='db.sqlite'
)

parser.add_argument(
    '--mapping', '-m',
    metavar='mapping.json',
    type=str,
    help='a mapping file allows to control how data is imported. For more details, refer to the examples',
    required=False
)

parser.add_argument(
    '--csv-has-title-columns', '-t',
    help="indicates if the first line in the CSV file contains title columns to be used as column tables. In the case of mapped foreign keys, they'll be used as table names",
    action='store_true',
    required=False,
    default=False
)

parser.add_argument(
    '--default-mapping-action', '-d',
    help='default action to take if an unmapped column (not in mapping file) is found while importing',
    type=str,
    required=False,
    default='ignore',
    choices=['import', 'ignore']
)

libcsv2sqlite.csv_to_sqlite3(parser.parse_args())
