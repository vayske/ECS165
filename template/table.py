from template.page import *
from BTrees.IIBTree import IIBTree

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
        self.page_directory = IIBTree() #Use IOBTree if you want offset within the page
        self.pages = [Page(),Page(),Page(),Page()] #Ready the initial Meta data columns
        self.page_directory.insert(START_RID, 4) #Preallocate the first set of columns
        for i in range(0, num_columns):
            self.pages.append(Page())
    def __merge(self):
        pass

