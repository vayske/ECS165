from lstore.table import *
import os
class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)
        pass

    def close(self):
        for table in self.tables:
            table.bufferpool.flush()
            file = open(table.name, 'w')
            file.write(table.name + '\n')
            file.write(str(table.num_columns) + '\n')
            file.write(str(table.key) + '\n')
            file.write(str(table.total_records) + '\n')
            file.write(str(table.base_updates) + '\n')
            file.write(str(table.page_directory) + '\n')
            file.close()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        os.makedirs(name)
        os.chdir(name)
        table = Table(name, num_columns, key)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        os.chdir(name)
        file = open(name, 'r')
        table_data = file.read().splitlines()
        table = Table(table_data[0], int(table_data[1]), int(table_data[2]))
        table.total_records = int(table_data[3])
        table.base_updates = eval(table_data[4])
        table.page_directory = eval(table_data[5])
        table.index.create_index(table.key, table)
        file.close()
        self.tables.append(table)
        return table
