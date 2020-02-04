from lstore.page import *
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
        self.key = key + 4
        self.num_columns = num_columns
        self.num_base_records = 0
        self.num_tail_records = 0
        self.total_records = 0
        self.base_records = []              # List of Page lists, Each position contains
        self.tail_records = []              # Indirection Page(base_records[i][INDIRECTION_COLUMN]),
        self.page_full = True               # RID Page(base_records[i][RID_COLUMN]), ETC

        self.page_directory = {}            # A Python Dictionary in the format {RID:(Page_Index, Slot), ...}
        pass

    def __merge(self):
        pass
 
