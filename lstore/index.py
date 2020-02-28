from BTrees.IOBTree import IOBTree
from lstore.bufferpool import Bufferpool

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.trees = []
        self.index = []
        for i in range(0, table.num_columns):
            self.trees.append(IOBTree())
            self.index.append(False)

    """
    # returns the location of all records with the given value on column "column"
    """
    def has_index(self, column):
        return self.index[column]

    def locate(self, column, value):
        RidList = None
        if self.trees[column].has_key(value):
            RidList = self.trees[column].get(value)
        return RidList

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def update_tree(self, column, old, new, rid):
        RidList = self.trees[column].get(old)
        RidList.remove(rid)
        new_list = []
        if len(RidList) > 0:
            self.trees[column].__setitem__(old, RidList)
        if self.trees[column].has_key(new):
            new_list = self.trees[column].get(new)
            new_list.append(rid)
            self.trees[column].__setitem__(new, new_list)
        else:
            new_list.append(rid)
            self.trees[column].insert(new, new_list)

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """
    def create_index(self, table, column, bufferpool):
        for i in range(0, table.total_records):
            page_index, slot = table.page_directory[i]
            page_name = "b_" + str(page_index) + "_" + "c_" + str(SCHEMA_ENCODING_COLUMN)
            schema = bufferpool.read(page_name, slot)
            value = None
            RidList = []
            if schema[column] == ord('1'):
                page_name = "b_" + str(page_index) + "_" + "c_" + str(INDIRECTION_COLUMN)
                indirection = int.from_bytes(bufferpool.read(page_name, slot), 'big', signed=True)
                tail_index = indirection // 512
                tail_slot = indirection % 512
                page_name = "t_" + str(tail_index) + "_" + "c_" + str(column+4)
                value = int.from_bytes(bufferpool.read(page_name, tail_slot), 'big', signed=True)
            else:
                page_name = "b_" + str(page_index) + "_" + "c_" + str(column+4)
                value = int.from_bytes(bufferpool.read(page_name, slot), 'big', signed=True)
            if column != table.key:
                if self.trees[column].has_key(value):
                    RidList = self.trees[column].get(value)
                    RidList.append(i)
                    self.trees[column].__setitem__(value, RidList)
                else:
                    RidList.append(i)
                    self.trees[column].insert(value, RidList)
            else:
                RidList.append(i)
                self.trees[column].insert(value, RidList)
        self.index[column] = True

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.trees[column_number].clear()
        self.index[column_number] = False
