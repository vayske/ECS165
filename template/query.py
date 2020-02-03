from template.table import *
from template.index import Index
from time import process_time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.currentRID = 0
        self.index = Index(self.table)
        self.has_index = False
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        if(self.has_index == False):
            self.index.create_index(self.table, self.table.key)
            self.has_index = True
        rid = self.index.remove(key)
        if(rid == None):
            print("Key Not Found")
            return None
        del self.table.page_directory[rid]
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        length = len(columns)
        if(length > 0):
            if(self.table.page_full):                       #If the Current Pages are Full
                new_base = []                               #Create New Base Pages for all field as a list
                new_tail = []
                for i in range(0, length+4):
                    new_base.append(Page())
                    new_tail.append(Page())
                self.table.num_base_records += 1
                self.table.num_tail_records += 1
                self.table.base_records.append(new_base)
                self.table.tail_records.append(new_tail)
                self.table.page_full = False
            base_pages_index = self.table.num_base_records - 1
            schema_encoding = '0' * length
            indirection_to_bytes = (0).to_bytes(8, 'big')
            schema_to_bytes = bytearray(8)
            schema_to_bytes[0:4] = bytearray(schema_encoding, 'utf-8')
            rid_to_bytes = self.currentRID.to_bytes(8, 'big')
            time_to_bytes = int(process_time()).to_bytes(8, 'big')
            self.table.base_records[base_pages_index][INDIRECTION_COLUMN].write(indirection_to_bytes)
            self.table.base_records[base_pages_index][RID_COLUMN].write(rid_to_bytes)
            self.table.base_records[base_pages_index][TIMESTAMP_COLUMN].write(time_to_bytes)
            self.table.base_records[base_pages_index][SCHEMA_ENCODING_COLUMN].write(schema_to_bytes)
            for i in range(self.table.key, length+4):
                value_to_bytes = columns[i-4].to_bytes(8, 'big')
                self.table.base_records[base_pages_index][i].write(value_to_bytes)
            slot = self.table.base_records[base_pages_index][0].num_records - 1
            self.table.page_directory[self.currentRID] = (base_pages_index, slot)
            self.currentRID += 1
            self.table.total_records += 1
            if(not(self.table.base_records[base_pages_index][0].has_capacity())):
                self.table.page_full = True
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        list = []
        if(self.has_index == False):
            self.index.create_index(self.table, self.table.key)
            self.has_index = True
        rid = self.index.locate(key)
        if(rid == None):
            print("Key Not Found\n")
            return None
        page_index, slot = self.table.page_directory[rid]
        for i in range(0, len(query_columns)):
            colunm_value_bytes = self.table.base_records[page_index][i+4].read(slot)
            query_columns[i] = int.from_bytes(colunm_value_bytes, 'big')
        schema_bytes = self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].read(slot)
        schema = schema_bytes[0:5].decode('utf-8')
        for i in range(0, self.table.num_columns):
            if(schema[i] == '1'):
                indirection_bytes = self.table.base_records[page_index][INDIRECTION_COLUMN].read(slot)
                indirection = int.from_bytes(indirection_bytes, 'big')
                updated_value_bytes = self.table.tail_records[page_index][i+4].read(indirection)
                updated_value = int.from_bytes(updated_value_bytes, 'big')
                query_columns[i] = updated_value
        record = Record(rid, key, query_columns)
        list.append(record)
        return list

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        if(self.has_index == False):
            self.index.create_index(self.table, self.table.key)
            self.has_index = True
        rid = self.index.locate(key)
        if(rid == None):
            print("Key Not Found")
            return None
        page_index, slot = self.table.page_directory[rid]
        new_schema_to_bytes = self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].read(slot)
        tail_indirection_to_bytes = self.table.base_records[page_index][INDIRECTION_COLUMN].read(slot)
        tail_indirection = int.from_bytes(tail_indirection_to_bytes, 'big')
        for i in range(0, len(columns)):
            value_to_bytes = bytearray(8)
            if(new_schema_to_bytes[i] == ord('1')):
                value_to_bytes = self.table.tail_records[page_index][i+4].read(tail_indirection)
            if(columns[i] != None):
                new_schema_to_bytes[i] = ord('1')
                value_to_bytes = columns[i].to_bytes(8, 'big')
            self.table.tail_records[page_index][i+4].write(value_to_bytes)
            new_tail_indirection = self.table.tail_records[page_index][i+4].num_records - 1
        tail_indirection_to_bytes = new_tail_indirection.to_bytes(8,'big')
        self.table.base_records[page_index][INDIRECTION_COLUMN].change_value(slot, tail_indirection_to_bytes)
        if(tail_indirection == 0):
            slot_to_bytes = slot.to_bytes(8,'big')
            self.table.tail_records[page_index][INDIRECTION_COLUMN].write(slot_to_bytes)
        else:
            self.table.tail_records[page_index][INDIRECTION_COLUMN].write(tail_indirection_to_bytes)
        rid_to_bytes = rid.to_bytes(8,'big')
        time_to_bytes = int(process_time()).to_bytes(8,'big')
        self.table.tail_records[page_index][RID_COLUMN].write(rid_to_bytes)
        self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].change_value(slot, new_schema_to_bytes)
        self.table.tail_records[page_index][SCHEMA_ENCODING_COLUMN].write(new_schema_to_bytes)
        self.table.tail_records[page_index][TIMESTAMP_COLUMN].write(time_to_bytes)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        if(self.has_index == False):
            self.index.create_index(self.table, self.table.key)
            self.has_index = True
        for key in range(start_range, end_range+1):
            rid = self.index.locate(key)
            if(rid == None):
                result += 0
                continue
            page_index, slot = self.table.page_directory[rid]
            schema_to_bytes = self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].read(slot)
            schema = schema_to_bytes[0:5].decode('utf-8')
            if(schema[aggregate_column_index] == '1'):
                indirection_to_bytes = self.table.base_records[page_index][INDIRECTION_COLUMN].read(slot)
                indirection = int.from_bytes(indirection_to_bytes, 'big')
                value_to_bytes = self.table.tail_records[page_index][aggregate_column_index+4].read(indirection)
                value = int.from_bytes(value_to_bytes, 'big')
            else:
                value_to_bytes = self.table.base_records[page_index][aggregate_column_index+4].read(slot)
                value = int.from_bytes(value_to_bytes, 'big')
            result += value
        return result
