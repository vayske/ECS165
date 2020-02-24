from lstore.table import Table
<<<<<<< HEAD
from lstore.page import Page
=======
from lstore.page import *
from collections import defaultdict
import os, sys, json
import numpy as np

class Bufferpool:

    def __init__(self, db):
        self.db = db
        self.empty = [i for i in range(90)]
        self.used = []
        self.pinned = np.zeros(90, dtype=int)
        self.LRUIndex = np.empty(90, dtype=int)
        self.pool = [Page("", ()) for i in range(90)]  # empty page, will be replace by real page
        self.directory = defaultdict(lambda: -1)  # (table, b or t, page_index, column_number) -> index in pool
        # -1 if not in pool(need to get from file)

    def flush_pool(self):
        for page in self.pool:
            if page.dirty:
                file = open(page.filename, "wb")
                data_str = page.data
                file.write(data_str)
                file.close()

    def get_from_disk(self, filename, meta):
        if os.stat(filename).st_size > 0:
            file = open(filename, "rb")
            data_str = file.read()
            page = Page(filename, meta)
            page.data = data_str
        else:
            page = Page(filename, meta)

        page.dirty = False;
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.LRUIndex[empty_index] = len(self.used) - 1
        self.write(empty_index, page=page)
        return empty_index

    def write(self, index, page=-1, value=-1):
        if(page != -1):
            self.pool[index] = page
        if value != -1:
            self.pool[index].write(value)

    def get(self, index):
        return self.pool[index]

    def evict(self):
        evict_index = 0
        for i in range(0, len(self.used)):  # Evict the least recently used and unpinned page
            if (self.pinned[self.used[i]] == 0):
                evict_index = self.used.pop(i)
                self.LRUIndex[evict_index] = -1
                break
        # evict oldest page
        self.empty.append(evict_index)
        if self.pool[evict_index].dirty:
            page = self.pool[evict_index]
            file = open(page.filename, "wb")
            data_str = page.data
            file.write(data_str)
            file.close()
        del self.directory[self.pool[evict_index].meta]

    def getindex(self, table, bt, page, column):
        i = self.directory[(table, bt, page, column)]
        if i == -1:
            if len(self.used) == len(self.pool):
                self.evict()
            path = os.getcwd() + "/" + table + "/" + bt + "_" + str(page) + "c_" + str(column)
            i = self.get_from_disk(path, (table, bt, page, column))
            self.directory.update({(table, bt, page, column): i})
        return i

>>>>>>> a11ab0b9adaf0978dc32a498d424b37ea59dc4fc

class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = Bufferpool(self)
        pass

    def open(self, path):
<<<<<<< HEAD
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

=======
        if not os.path.exist(path):
            os.makedirs(path)
        os.chdir(path)

    def close(self):
        for table in self.tables.items():
            table.bufferpool.flush_pool()
            table.write_meta_to_disk()
>>>>>>> a11ab0b9adaf0978dc32a498d424b37ea59dc4fc
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
<<<<<<< HEAD
    def get_table(self, name):
        for table in self.tables:
            if (table.name == name):
                return table
=======
>>>>>>> a11ab0b9adaf0978dc32a498d424b37ea59dc4fc

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bufferpool,0,0,0)
        self.tables.update({name: table})
        return table

    def get_table(self, name):
        with open(os.getcwd() + '/' + name + '/metadata.json', 'r') as fp:
            meta_dict = json.load(fp.read())
        fp.close()
        table = Table(name, meta_dict['num_column'], meta_dict['key'], self.bufferpool, meta_dict['num_basepage'],
                      meta_dict['num_tailpage'], meta_dict['total_records'])
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        del self.tables[name]
        pass
