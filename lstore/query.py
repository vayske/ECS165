from lstore.table import *
from lstore.index import Index
from lstore.bufferpool import *
from lstore.counter import *
from time import process_time
import os
import threading

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.bufferpool = self.table.bufferpool

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key, transaction = None, undo = True):
        if undo:
            #recover deleted record
            rid = key
            #more undo code 
        if not self.table.index.has_index(self.table.key):
            self.table.index.create_index(self.table, self.table.key, self.bufferpool)
        RidList = self.table.index.locate(self.table.key, key)
        for rid in RidList:
            if not self.table.lock_manager.get_xlock(rid, transaction):
                return (-1, False)
           del self.table.page_directory[rid]
        return (rid, False)

    """
    # Insert a record with specified columns
    """
    def write_helper(self, page_type, page_index, column, value):
        value_in_bytes = None
        if isinstance(value, str):
            value += "000"
            value_in_bytes = value.encode('utf-8')
        elif isinstance(value, int):
            value_in_bytes = value.to_bytes(8, 'big', signed=True)
        page_name = self.get_name(page_type, page_index, column)
        self.bufferpool.write(page_name, value_in_bytes)

    def get_name(self, bt, index, column_num):
        return bt + "_" + str(index) + "_" + "c_" + str(column_num)

    def insert(self, *columns, transaction = None, undo = False):
        if undo:
            #undo any thing was done, invalidate inserted record
            rid = columns[0]
            key = columns[1]
            #more undo code
        base_index = self.table.total_records // 512
        indirection = -1
        rid = self.table.total_records
        if not self.table.lock_manager.get_xlock(rid, transaction):
            return (-1, 0, False)
        schema_encoding = '0' * self.table.num_columns
        time = int(process_time())
        insertlist = [indirection, rid, time, schema_encoding] + list(columns)
        column = 0
        for value in insertlist:
            self.write_helper("b", base_index, column, value)
            column += 1
        slot = self.table.total_records % 512
        self.table.total_records += 1
        self.table.page_directory[rid] = (base_index, slot)
        return (rid, columns[0], True)  #rid, columns[0] = key index(student ID), used for undo

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

    def select(self, key, column, query_columns, transaction = None, undo = False):
        if not self.table.index.has_index(column):
            self.table.index.create_index(self.table, column,  self.bufferpool)
        record_list = []
        RidList = self.table.index.locate(column, key)
        for rid in RidList:
            select_list = []
            try:
                page_index, slot = self.table.page_directory[rid]
            except KeyError:
                continue
            if not self.table.lock_manager.get_slock(rid, transaction):
                return ([], False)
            for i in range(0, len(query_columns)):
                if query_columns[i] == 1:
                    page_name = self.get_name("b", page_index, i+4)
                    value_in_bytes = self.bufferpool.read(page_name, slot)
                    select_list.append(int.from_bytes(value_in_bytes, 'big', signed=True))
                else:
                    select_list.append(None)
            page_name = self.get_name("b", page_index, INDIRECTION_COLUMN)
            indirection = int.from_bytes(self.bufferpool.read(page_name, slot), 'big', signed=True)
            if indirection == -1:
                record_list.append(Record(rid, key, select_list))
            else:
                page_name = self.get_name("b", page_index, SCHEMA_ENCODING_COLUMN)
                schema_in_bytes = self.bufferpool.read(page_name, slot)
                tail_index = indirection // 512
                tail_slot = indirection % 512
                for i in range(0, self.table.num_columns):
                    if schema_in_bytes[i] == ord('1') and query_columns[i] == 1:
                        page_name = self.get_name("t", tail_index, i+4)
                        updated_value = int.from_bytes(self.bufferpool.read(page_name, tail_slot), 'big', signed=True)
                        select_list[i] = updated_value
                record_list.append(Record(rid, key, select_list))
        return (record_list, True)




    """   
    # Update a record with specified key and columns
    """

    def update(self, key, *columns, transaction = None, undo = False):
        #if undo:
            #undo update, invalidate new update, change back indirection 
            rid = columns[0]
            old_indirection = columns[1]
            #more undo code
        if not self.table.index.has_index(self.table.key):
            self.table.index.create_index(self.table, self.table.key, self.bufferpool)
        RidList = self.table.index.locate(self.table.key, key)
        for rid in RidList:
            try:
                page_index, slot = self.table.page_directory[rid]
            except KeyError:
                continue
            if not self.table.lock_manager.get_Xlock(rid, transaction):
                return (-1, -1, False)
            page_name = self.get_name("b", page_index, INDIRECTION_COLUMN)
            indirection = int.from_bytes(self.bufferpool.read(page_name, slot), 'big', signed=True)
            old_indirection = indirection
            page_name = self.get_name("b", page_index, SCHEMA_ENCODING_COLUMN)
            schema_in_bytes = self.bufferpool.read(page_name, slot)
            record_list = [-1, -1, -1, -1, -1]
            old_value = None
            for i in range(0, len(columns)):
                if schema_in_bytes[i] == ord('1'):
                    tail_index = indirection // 512
                    tail_slot = indirection % 512
                    page_name = self.get_name("t", tail_index, i+4)
                    record_list[i] = int.from_bytes(self.bufferpool.read(page_name, tail_slot), 'big', signed=True)
                if columns[i] is not None:
                    schema_in_bytes[i] = ord('1')
                    old_value = record_list[i]
                    if old_value == -1:
                        page_name = self.get_name("b", page_index, i+4)
                        old_value = int.from_bytes(self.bufferpool.read(page_name, slot), 'big', signed=True)
                    record_list[i] = columns[i]
                    if not self.table.index.has_index(i):
                        self.table.index.create_index(self.table, i, self.bufferpool)
                    self.table.index.update_tree(i, old_value, record_list[i], rid)
            tail_index = self.table.total_updates // 512
            tail_indirection = indirection
            tail_rid = rid
            tail_time = int(process_time())
            tail_schema = schema_in_bytes[0:5].decode('utf-8')
            update_list = [tail_indirection, tail_rid, tail_time, tail_schema] + record_list
            column = 0
            for value in update_list:
                self.write_helper("t", tail_index, column, value)
                column += 1
            base_indirection = self.table.total_updates.to_bytes(8, 'big', signed=True)
            page_name = "b_" + str(page_index) + "_" + "c_" + str(INDIRECTION_COLUMN)
            self.bufferpool.change_value(page_name, slot, base_indirection)
            base_schema = schema_in_bytes
            page_name = "b_" + str(page_index) + "_" + "c_" + str(SCHEMA_ENCODING_COLUMN)
            self.bufferpool.change_value(page_name, slot, base_schema)
            self.table.total_updates += 1
            if self.table.total_updates > 50000 and not self.table.start_merge:
                self.table.start_merge = True
                t = threading.Thread(target=self.table.merge, args=(self.bufferpool,))
                t.start()
            return (rid, old_indirection, True)



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    """

    def sum(self, start_range, end_range, aggregate_column_index, transaction = None, undo = False):
        result = 0
        if not self.table.index.has_index(aggregate_column_index):
            self.table.index.create_index(self.table, aggregate_column_index, self.bufferpool)
        for i in range(start_range, end_range+1):
            RidList = self.table.index.locate(aggregate_column_index, i)
            if RidList == None:
                result += 0
                continue
            for rid in RidList:
                try:
                    page_index, slot = self.table.page_directory[rid]
                except KeyError:
                    continue
                if not self.table.lock_manager.get_slock(rid, transaction):
                    return (-1, False)
                page_name = self.get_name("b", page_index, SCHEMA_ENCODING_COLUMN)
                schema = self.bufferpool.read(page_name, slot)
                if schema[aggregate_column_index] == ord('1'):
                    page_name = self.get_name("b", page_index, INDIRECTION_COLUMN)
                    indirection = int.from_bytes(self.bufferpool.read(page_name, slot), 'big', signed=True)
                    tail_index = indirection // 512
                    tail_slot = indirection % 512
                    page_name = self.get_name("t", tail_index, aggregate_column_index+4)
                    value = int.from_bytes(self.bufferpool.read(page_name, tail_slot), 'big', signed=True)
                    result += value
                else:
                    page_name = self.get_name("b", page_index, aggregate_column_index+4)
                    value = int.from_bytes(self.bufferpool.read(page_name, slot), 'big', signed=True)
                    result += value
        return (result, True)

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, transaction = None, undo = False):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns, transaction)
            return u[-1]
        return False



