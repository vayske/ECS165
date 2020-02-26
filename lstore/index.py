from lstore.table import *

from BTrees.IOBTree import IOBTree
#import lstore.query as query

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
        #print("In locate function: trying to locate value:" + str(value))
        #print("got rid: " + str(ridList))
        return ridList
    """
    # optional: Create index on specific column
    """
    def remove(self, column, value):
        return self.trees[column].pop(value, None)

    def create_index(self, table):
        print("in create indexs function")
        # --- Loop All Data to create Tree --- #
        #rid_index = table.bufferpool.getindex(table.name, "b", 0, 4)
        for i in range(0, table.num_base_page):
            rid_index = table.bufferpool.getindex(table.name, "b", i, RID_COLUMN)
            ind_index = table.bufferpool.getindex(table.name, "b", i, INDIRECTION_COLUMN)
            for j in range(0, table.bufferpool.get(rid_index).num_records):
                rid = int.from_bytes(table.bufferpool.get(rid_index).read(j), 'big')
                if rid == -1:
                    continue
                indirection = int.from_bytes(table.bufferpool.get(ind_index).read(j),'big')
                new_column = []
                for k in range(table.num_columns):
                    latest_index = table.bufferpool.getindex(table.name, "t", i, k + table.key)
                    val = int.from_bytes(table.bufferpool.get(latest_index).read(indirection), 'big')
                    if (self.trees[k].has_key(val)):
                        tempList = self.trees[k].get(val)
                        tempList.append(rid)
                        self.trees[k].__setitem__(val, tempList)
                    else:
                        self.trees[k].insert(val, [rid])

                #new_column = query.Query.getLatestRecord(rid, table.num_columns)            
                pass
    
    def create_keyindex(self, table):
        print("in create keyindex function")
        for i in range(0, table.num_base_page):
            rid_index = table.bufferpool.getindex(table.name, "b", i, RID_COLUMN)
            key_index = table.bufferpool.getindex(table.name, "b", i, table.key)
            for j in range(0, table.bufferpool.get(rid_index).num_records):
                rid = int.from_bytes(table.bufferpool.get(rid_index).read(j), 'big')
                key_val = int.from_bytes(table.bufferpool.get(key_index).read(j),'big')
                #print("rid: " + str(rid) + " key_val: " + str(key_val))
                if rid == -1:
                    continue
                #print("try to locate before insert")
                #rid_list = self.locate(table.key-4, key_val)
                #print(rid_list)
                self.trees[table.key-4].insert(key_val, [rid])
                #print("rid inserted. now try to locate it")  
                #rid_list =  self.locate(table.key-4, key_val)
                #print(rid_list)
                
    """"
    # optional: Drop index of specific column
    """""

    def drop_index(self, column_number):
        self.trees[column_number].clear()
        pass
