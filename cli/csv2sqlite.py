import argparse
import libcsv2sqlite


parser = argparse.ArgumentParser(
    description='Generate databases or import data into existing databases.'
)

parser.add_argument(
    '--input', '-i',
    metavar='data.csv',
    type=str,
    help='Path of the origin data file',
    required=True
)

parser.add_argument(
    '--output', '-o',
    metavar='db.sqlite',
    type=str,
    help='Path of the destination database file.',
    required=False,
    default='db.sqlite'
)

parser.add_argument(
    '--mapping', '-m',
    metavar='mapping.json',
    type=str,
    help='Path of the JSON mapping file, which describes how to import data',
    required=False
)

parser.add_argument(
    '--csv-has-title-columns', '-t',
    help='Indicates if the first line in the CSV file contains title columns',
    action='store_true',
    required=False,
    default=False
)

parser.add_argument(
    '--default-mapping-action', '-d',
    help='Action to take on unmapped columns',
    type=str,
    required=False,
    default='ignore',
    choices=['import', 'ignore']
)

libcsv2sqlite.csv_to_sqlite3(parser.parse_args())
