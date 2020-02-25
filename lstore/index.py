from lstore.table import *
from BTrees.IOBTree import IOBTree
import lstore.query as query
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, tree_num):
        self.trees = []
        for i in range(0, tree_num):
            self.trees.append(IOBTree())
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, column, value):
        ridList = self.trees[column].get(value)
        return ridList
    """
    # optional: Create index on specific column
    """
    def remove(self, column, value):
        return self.trees[column].pop(value, None)

    def create_index(self, table):
        # --- Loop All Data to create Tree --- #
        for i in range(table.num_base_page):
            rid_index = table.bufferpool.getindex(table.name, "b", i, RID_COLUMN)
            ind_index = table.bufferpool.getindex(table.name, "b", i, INDIRECTION_COLUMN)
            for j in range(table.bufferpool.get(rid_index).num_records):
                rid = int.from_bytes(table.bufferpool.get(rid_index).read(j), 'big')
                if rid == -1:
                    continue
                indirection = int.from_bytes(table.bufferpool.get(ind_index).read(j),'big')
                new_column = []
                for k in range(0, table.num_columns):
                    latest_index = table.bufferpool.getindex(table.name, "t", i, k)
                    val = int.from_bytes(table.bufferpool.get(latest_index).read(indirection), 'big')
                    if (self.index.trees[k].has_key(val)):
                        tempList = self.index.trees[k].get(val)
                        tempList.append(self.currentRID)
                        self.index.trees[k].__setitem__(val, tempList)
                    else:
                        self.index.trees[k].insert(val, [rid])

                #new_column = query.Query.getLatestRecord(rid, table.num_columns)            
                pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.trees[column_number].clear()
        pass
