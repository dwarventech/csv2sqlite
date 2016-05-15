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
    metavar='db.sqlite3',
    type=str,
    help='Path of the destination database file.',
    required=False,
    default='db.sqlite3'
)
     
parser.add_argument(
    '--mapping', '-m',
    metavar='mapping.json',
    type=str,
    help='Path of the JSON mapping file, which describes how to import data',
    required=True
)

parser.add_argument(
    '--csv-has-title-columns', '-c',
    help='Indicates if the first line in the CSV file contains title columns',
    action='store_true',
    required=False,
    default=True
)

args = parser.parse_args()

libcsv2sqlite.csv_to_sqlite3(
    args.input,
    args.mapping,
    args.output,
    args.csv_has_title_columns)