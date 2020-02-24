from lstore.table import *
from BTrees.IIBTree import IIBTree
import lstore.query as query
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, tree_num):
        self.trees = []
        for i in range(0, tree_num):
            self.trees.append(IIBTree())
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, column,value):
        return self.trees[column].get(value)
    """
    # optional: Create index on specific column
    """
    def remove(self, column, value):
        return self.trees[column].pop(value, None)

    def create_index(self, table, column_number):
        # --- Loop All Data to create Tree --- #
        for i in range(table.num_base_page):
            index1 = table.bufferpool.getindex(table.name, "b", i, RID_COLUMN)
            index2 = table.bufferpool.getindex(table.name, "b", i, column_number)
            for j in range(table.bufferpool.get(index1).num_records):
                rid = int.from_bytes(table.bufferpool.get(index1).read(j), 'big')
                if rid == -1:
                    continue
                new_column = query.Query.getLatestRecord(rid, table.num_columns)
                for i in range(0, table.num_columns):
                    self.trees[i].insert(new_column[i], rid)

                pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.trees[column_number].clear()
        pass
