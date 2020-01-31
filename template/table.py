from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key - 1
        self.num_columns = num_columns
        self.num_base_records = 0
        self.num_tail_records = 0
        self.total_records = 0
        self.base_records = []
        self.tail_records = []
        self.page_full = True
        self.page_directory = {}
        pass

    def __merge(self):
        pass
 
