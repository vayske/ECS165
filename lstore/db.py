from lstore.table import Table
from lstore.page import Page

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        try:
            with open('disk.txt', 'x') as fout:
                return 0
        except FileExistsError:
            f = open('disk.txt', 'r')
        num_tables = f.readline()
        if(num_tables == ""):
            f.close()
            return 0
        else:
            num_tables = int(num_tables)
        for i in range(0, num_tables):
            disk = f.read().splitlines()
            name = disk[0]
            key = int(disk[1])
            num_columns = int(disk[2])
            table = Table(name, num_columns, key)
            table.num_base_records = int(disk[3])
            table.num_tail_records = int(disk[4])
            table.total_records = int(disk[5])
            table.page_full = bool(disk[6])
            table.page_directory = eval(disk[7])
            num_base_columns = int(disk[8])
            num_tail_columns = int(disk[9])
            offset = 9
            for j in range(0, table.num_base_records):
                base = []
                for k in range(0, num_base_columns):
                    page = Page()
                    offset = offset + 1
                    page.num_records = int(disk[offset])
                    offset = offset + 1
                    page.data = eval(disk[offset])
                    base.append(page)
                table.base_records.append(base)
            for j in range(0, table.num_tail_records):
                tail = []
                for k in range(0, num_tail_columns):
                    page = Page()
                    offset = offset + 1
                    page.num_records = int(disk[offset])
                    offset = offset + 1
                    page.data = eval(disk[offset])
                    tail.append(page)
                table.tail_records.append(tail)
            self.tables.append(table)
        f.close()
        pass

    def close(self):
        f = open('disk.txt', 'w+')
        f.write(str(len(self.tables)) + "\n")
        for table in self.tables:
            f.write(table.name + "\n")
            f.write(str(table.key) + "\n")
            f.write(str(table.num_columns) + "\n")
            f.write(str(table.num_base_records) + "\n")
            f.write(str(table.num_tail_records) + "\n")
            f.write(str(table.total_records) + "\n")
            f.write(str(table.page_full) + "\n")
            f.write(str(table.page_directory) + "\n")
            f.write(str(len(table.base_records[0])) + "\n")
            f.write(str(len(table.tail_records[0])) + "\n")
            for j in range(0, table.num_base_records):
                for page in table.base_records[j]:
                    f.write(str(page.num_records) + "\n")
                    f.write(str(page.data) + "\n")
            for j in range(0, table.num_tail_records):
                for page in table.tail_records[j]:
                    f.write(str(page.num_records) + "\n")
                    f.write(str(page.data) + "\n")
        f.close()

        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def get_table(self, name):
        for table in self.tables:
            if (table.name == name):
                return table

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
