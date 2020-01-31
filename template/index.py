from template.table import *
from BTrees.IIBTree import IIBTree
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.tree = IIBTree()
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):
        for i in range(0, table.num_base_records):
            for j in range(0, table.base_records[i][column_number].num_records):
                self.tree.insert(table.base_records[i][RID_COLUMN].data[j], table.base_records[i][column_number])
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass
