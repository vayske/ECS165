from BTrees.IOBTree import IOBTree
from lstore.config import *
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.indices[table.key] = IOBTree()
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return self.indices[column].get(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number, table):
        self.indices[column_number] = IOBTree()
        for rid in range(table.total_records):
            page_index, slot = table.page_directory[rid]
            page_name = 'b_' + str(page_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
            poolindex = table.bufferpool.get_page(page_name)
            schema = table.bufferpool.pool[poolindex].read(slot, 1)
            value = None
            if schema[column_number] == '1':
                page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
                poolindex = table.bufferpool.get_page(page_name)
                base_indirection = table.bufferpool.pool[poolindex].read(slot)
                tail_index = base_indirection // PAGESIZE
                tail_slot = base_indirection % PAGESIZE
                page_name = 't_' + str(page_index) + '_' + str(tail_index) + '_c_' + str(column_number+4)
                poolindex = table.bufferpool.get_page(page_name)
                value = table.bufferpool.pool[poolindex].read(tail_slot)
            else:
                page_name = 'b_' + str(page_index) + '_c_' + str(column_number+4)
                poolindex = table.bufferpool.get_page(page_name)
                value = table.bufferpool.pool[poolindex].read(slot)
            if self.indices[column_number].has_key(value):
                RidList = self.indices[column_number].get(value)
                RidList.append(rid)
                self.indices[column_number].__setitem__(value, RidList)
            else:
                self.indices[column_number].insert(value, [rid])
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass
