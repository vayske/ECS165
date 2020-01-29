from template.page import *
from BTrees.IIBTree import IIBTree

import numpy as np

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
        self.key = key
        self.num_columns = num_columns
        schema_column = np.zeros(num_columns)
        self.page_directory = IIBTree()
        self.pages = [Page(),Page(),Page(),Page()]

    def __merge(self):
        pass

