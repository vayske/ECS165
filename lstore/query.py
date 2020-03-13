from lstore.table import *
from lstore.index import Index
from lstore.config import *
from time import process_time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        RidList = self.table.index.locate(self.table.key, key)
        if RidList is None:
            return False
        rid = RidList[0]
        if not self.table.lock_manage.lock_write(rid):
            return False
        self.table.index.indices[self.table.key].__delitem__(key)
        del self.table.page_directory[rid]
        self.table.lock_manage.unlock_write(rid)
        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, undo=False):
        if undo:
            while not self.delete(columns[0]):
                pass
            return True
        if self.table.total_records % PAGESIZE == 0:
            self.table.base_updates.append(0)
        indirection = -1
        rid = self.table.total_records
        time = int(process_time())
        schema_encoding = '0' * self.table.num_columns
        base_index = self.table.total_records // 512
        slot = self.table.total_records % 512
        insert_list = [indirection, rid, time, schema_encoding] + list(columns)
        for i in range(len(insert_list)):
            page_name = 'b_' + str(base_index) + '_c_' + str(i)
            poolindex = self.table.bufferpool.get_page(page_name)
            self.table.bufferpool.pool[poolindex].write(insert_list[i])
        self.table.page_directory[rid] = (base_index, slot)
        self.table.index.indices[self.table.key].insert(columns[0], [rid])
        self.table.total_records += 1
        return True
        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns, undo=False):
        if undo:
            return True
        record_list = []
        RidList = self.table.index.locate(column, key)
        for rid in RidList:
            if not self.table.lock_manage.lock_read(rid):
                return False
            record = []
            page_index, slot = self.table.page_directory[rid]
            tps = 0
            for i in range(len(query_columns)):
                if query_columns[i] == 1:
                    page_name = 'b_' + str(page_index) + '_c_' + str(i+4)
                    poolindex = self.table.bufferpool.get_page(page_name)
                    value = self.table.bufferpool.pool[poolindex].read(slot)
                    tps = self.table.bufferpool.pool[poolindex].tps
                    record.append(value)
                else:
                    record.append(None)
            page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
            poolindex = self.table.bufferpool.get_page(page_name)
            indirection = self.table.bufferpool.pool[poolindex].read(slot)
            if indirection == -1:
                record_list.append(Record(rid, key, record))
            elif indirection < tps:
                record_list.append(Record(rid, key, record))
            else:
                page_name = 'b_' + str(page_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
                poolindex = self.table.bufferpool.get_page(page_name)
                schema = self.table.bufferpool.pool[poolindex].read(slot, 1)
                tail_index = indirection // 512
                tail_slot = indirection % 512
                for i in range(self.table.num_columns):
                    if schema[i] == '1' and query_columns[i] == 1:
                        page_name = 't_' + str(page_index) + '_' + str(tail_index) + '_c_' + str(i+4)
                        poolindex = self.table.bufferpool.get_page(page_name)
                        updated_value = self.table.bufferpool.pool[poolindex].read(tail_slot)
                        record[i] = updated_value
                record_list.append(Record(rid, key, record))
            self.table.lock_manage.unlock_read(rid)
        return record_list
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns, undo=False):
        RidList = self.table.index.locate(self.table.key, key)
        if undo:
            self.undo_update(key)
            return True
        for rid in RidList:
            if not self.table.lock_manage.lock_write(rid):
                return False
            page_index, slot = self.table.page_directory[rid]
            page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
            poolindex = self.table.bufferpool.get_page(page_name)
            base_indirection = self.table.bufferpool.pool[poolindex].read(slot)
            page_name = 'b_' + str(page_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
            poolindex = self.table.bufferpool.get_page(page_name)
            schema = self.table.bufferpool.pool[poolindex].read(slot, 1)
            update_list = [-1, -1, -1, -1, -1]
            for i in range(self.table.num_columns):
                if schema[i] == '1':
                    tail_index = base_indirection // PAGESIZE
                    tail_slot = base_indirection % PAGESIZE
                    page_name = 't_' + str(page_index) + '_' + str(tail_index) + '_c_' + str(i+4)
                    poolindex = self.table.bufferpool.get_page(page_name)
                    update_list[i] = self.table.bufferpool.pool[poolindex].read(tail_slot)
                if columns[i] is not None:
                    schema = schema[:i] + '1' + schema[i+1:]
                    update_list[i] = columns[i]
            tail_index = self.table.base_updates[page_index] // PAGESIZE
            update_column = [base_indirection, rid, int(process_time()), schema] + update_list
            for i in range(len(update_column)):
                page_name = 't_' + str(page_index) + '_' + str(tail_index) + '_c_' + str(i)
                poolindex = self.table.bufferpool.get_page(page_name)
                self.table.bufferpool.pool[poolindex].write(update_column[i])
            base_indirection = self.table.base_updates[page_index]
            page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
            poolindex = self.table.bufferpool.get_page(page_name)
            self.table.bufferpool.pool[poolindex].modify(slot, base_indirection)
            page_name = 'b_' + str(page_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
            poolindex = self.table.bufferpool.get_page(page_name)
            self.table.bufferpool.pool[poolindex].modify(slot, schema)
            self.table.base_updates[page_index] += 1
            self.table.lock_manage.unlock_write(rid)
        return True
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        NoRecord = True
        for i in range(start_range, end_range+1):
            RidList = self.table.index.locate(self.table.key, i)
            if RidList == None:
                result += 0
                continue
            NoRecord = False
            for rid in RidList:
                page_index, slot = self.table.page_directory[rid]
                page_name = 'b_' + str(page_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
                poolindex = self.table.bufferpool.get_page(page_name)
                schema = self.table.bufferpool.pool[poolindex].read(slot, 1)
                if schema[aggregate_column_index] == '1':
                    page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
                    poolindex = self.table.bufferpool.get_page(page_name)
                    base_indirection = self.table.bufferpool.pool[poolindex].read(slot)
                    tail_index = base_indirection // PAGESIZE
                    tail_slot = base_indirection % PAGESIZE
                    page_name = 't_' + str(page_index) + '_' + str(tail_index) + '_c_' + str(aggregate_column_index+4)
                    poolindex = self.table.bufferpool.get_page(page_name)
                    updated_value = self.table.bufferpool.pool[poolindex].read(tail_slot)
                    result += updated_value
                else:
                    page_name = 'b_' + str(page_index) + '_c_' + str(aggregate_column_index+4)
                    poolindex = self.table.bufferpool.get_page(page_name)
                    value = self.table.bufferpool.pool[poolindex].read(slot)
                    result += value
        if NoRecord:
            return False
        else:
            return result
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, undo=False):
        if undo:
            self.undo_update(key)
            return True
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def undo_update(self, key):
        RidList = self.table.index.locate(self.table.key, key)
        if RidList is None:
            return True
        rid = RidList[0]
        while not self.table.lock_manage.lock_write(rid):
            pass
        page_index, slot = self.table.page_directory[rid]
        page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
        base_indirection = self.table.bufferpool.pool[self.table.bufferpool.get_page(page_name)].read(slot)
        tail_index = base_indirection // PAGESIZE
        tail_slot = base_indirection % PAGESIZE
        page_name = 't_' + str(page_index) + '_' + str(tail_index) + '_c_' + str(INDIRECTION_COLUMN)
        tail_indirection = self.table.bufferpool.pool[self.table.bufferpool.get_page(page_name)].read(tail_slot)
        schema = None
        if tail_indirection == -1:
            schema = '00000'
        else:
            previous_tail_index = tail_indirection // PAGESIZE
            previous_tail_slot = tail_indirection % PAGESIZE
            page_name = 't_' + str(page_index) + '_' + str(previous_tail_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
            schema = self.table.bufferpool.pool[self.table.bufferpool.get_page(page_name)].read(previous_tail_slot, 1)
        page_name = 'b_' + str(page_index) + '_c_' + str(INDIRECTION_COLUMN)
        self.table.bufferpool.pool[self.table.bufferpool.get_page(page_name)].modify(slot, tail_indirection)
        page_name = 'b_' + str(page_index) + '_c_' + str(SCHEMA_ENCODING_COLUMN)
        self.table.bufferpool.pool[self.table.bufferpool.get_page(page_name)].modify(slot, schema)
        self.table.lock_manage.unlock_write(rid)
        return True


