class CsvColumnNotFound(Exception):
    def __init__(self, line_number, column_index):
        self.line_number = line_number
        self.column_index = column_index
