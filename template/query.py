from template.table import *
from template.index import Index
from template.config import *
from time import process_time
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.currentRID = START_RID
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
        if len(self.table.page_directory) == 0:
            self.table.page_directory.insert(self.currentRID, 4)
            for i in range(0, len(columns)):
                self.table.pages.append(Page())

        pageIndex = self.table.page_directory.get(self.table.page_directory.maxKey(self.currentRID))
        if not self.table.pages[pageIndex].has_capacity():
            self.table.page_directory.insert(int(self.currentRID), 4 + self.table.num_columns)
            for i in range(0, len(columns) + 4):
                self.table.pages.append(Page())

        self.table.pages[pageIndex - 4 + RID_COLUMN].write(self.currentRID)
        self.table.pages[pageIndex - 4 + TIMESTAMP_COLUMN].write(process_time())
        self.table.pages[pageIndex - 4 + SCHEMA_ENCODING_COLUMN].write('0' * self.table.num_columns)
        for i in range(0, len(columns)):
            self.table.pages[pageIndex + i].write(columns[i])

        self.currentRID = self.currentRID + 1




    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
