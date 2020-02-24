from lstore.table import *
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
        return self.tree.get(value)
    """
    # optional: Create index on specific column
    """
    def remove(self, value):
        return self.tree.pop(value, None)

    def create_index(self, table, column_number):
        # --- Loop All Data to create Tree --- #
        for i in range(0, table.num_base_page):
            for j in range(0, table.base_records[i][column_number].num_records):
                index1 = table.bufferpool.getindex(table.name, "b", i, RID_COLUMN)
                rid = int.from_bytes(table.bufferpool.get(index1).read(j), 'big')
                index2 = table.bufferpool.getindex(table.name, "b", i, column_number)
                key = int.from_bytes(table.bufferpool.get(index2).read(j), 'big')
                self.tree.insert(key, rid)
                pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        self.tree.clear()
        pass
