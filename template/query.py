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
        self.index = Index()
        self.has_index = False
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
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
                self.table.num_columns = length + 4
                self.table.num_base_records += 1
                self.table.num_tail_records += 1
                self.table.base_records.append(new_base)
                self.table.tail_records.append(new_tail)
                self.table.page_full = False
            base_pages_index = self.table.num_base_records - 1
            schema_encoding = '0' * length
            self.table.base_records[base_pages_index][INDIRECTION_COLUMN].write(-1)
            self.table.base_records[base_pages_index][RID_COLUMN].write(self.currentRID)
            self.table.base_records[base_pages_index][TIMESTAMP_COLUMN].write(process_time())
            self.table.base_records[base_pages_index][SCHEMA_ENCODING_COLUMN].write(schema_encoding)
            self.table.base_records[base_pages_index][self.table.key].write(columns[0])
            for i in range(self.table.key+1, length+4):
                self.table.base_records[base_pages_index][i].write(columns[i-4])
            self.table.page_directory[self.currentRID] = (base_pages_index, self.table.base_records[base_pages_index][0].num_records-1)
            self.currentRID += 1
            self.table.total_records += 1
            if(not(self.table.base_records[base_pages_index][0].has_capacity())):
                self.table.page_full = True
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        if(self.has_index == False):
            self.index.create_index(self.table, self.table.key)
        rid = self.index.locate(key)
        page_index, slot = self.table.page_directory[rid]
        new_schema = ''
        tail_indirection = 0
        for i in range(0, len(columns)):
            if(columns[i] != None):
                new_schema += '1'
            else
                new_schema += '0'
            self.table.tail_records[page_index][i+4].write(columns[i])
            tail_indirection = self.table.tail_records[page_index][i+4].num_records - 1
        self.table.base_records[page_index][INDIRECTION_COLUMN].change_value(slot, tail_indirection)
        if(tail_indirection == 0):
            self.table.tail_records[page_index][INDIRECTION_COLUMN].write(slot)
        else:
            self.table.tail_records[page_index][INDIRECTION_COLUMN].write(tail_indirection-1)
        self.table.tail_records[page_index][RID_COLUMN].write(rid)
        self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].change_value(new_schema)
        self.table.tail_records[page_index][SCHEMA_ENCODING_COLUMN].write(new_schema)
        self.table.tail_records[page_index][TIMESTAMP_COLUMN].write(process_time())
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
