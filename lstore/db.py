from lstore.table import Table
from lstore.bufferpool import Bufferpool
import os

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool()

    def open(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)


    def close(self):
        for table in self.tables:
            table.bufferpool.flush()
            file = open(table.name, "w")
            file.write(table.name + "\n")
            file.write(str(table.num_columns) + "\n")
            file.write(str(table.key) + "\n")
            file.write(str(table.total_records) + "\n")
            file.write(str(table.total_updates) + "\n")
            file.write(str(table.lineage) + "\n")
            file.write(str(table.page_directory) + "\n")
            file.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        os.makedirs(name)
        os.chdir(name)
        table = Table(name, num_columns, key, self.bufferpool)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in (0, len(self.tables)):
            if self.tables[i].name == name:
                self.tables.remove(self.tables[i])

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        os.chdir(name)
        file = open(name, "r")
        table_data = file.read().splitlines()
        tname = table_data[0]
        num_columns = int(table_data[1])
        key = int(table_data[2])
        table = Table(tname, num_columns, key, self.bufferpool)
        table.total_records = int(table_data[3])
        table.total_updates = int(table_data[4])
        table.lineage = int(table_data[5])
        table.page_directory = eval(table_data[6])
        file.close()
        self.tables.append(table)
        return table
