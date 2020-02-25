from lstore.table import Table
from lstore.page import *
from collections import defaultdict
import os, sys, json
import numpy as np

class Bufferpool:

    def __init__(self):
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
                num_records_to_bytes = page.num_records.to_bytes(8,'big')
                lineage_to_bytes = page.lineage.to_bytes(8,'big')
                #data_str = num_records_to_bytes + lineage_to_bytes + page.data
                data_str = page.data
                file.write(data_str)
                file.close()

    def get_from_disk(self, filename, meta):
        if os.stat(filename).st_size > 0:
            file = open(filename, "rb")
            data_str = file.read()
            page = Page(filename, meta)
            #page.num_records = int.from_bytes(data_str[0:8],'big')
            #page.lineage = int.from_bytes(data_str[8:16],'big')
            #print(page.num_records, page.lineage)
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
            num_records_to_bytes = page.num_records.to_bytes(8,'big')
            lineage_to_bytes = page.lineage.to_bytes(8,'big')
            #data_str = num_records_to_bytes + lineage_to_bytes + page.data
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


class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = Bufferpool()
        pass

    def open(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)

    def close(self):
        for _, table in self.tables.items():
            table.bufferpool.flush_pool()
            table.write_meta_to_disk()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bufferpool,0,0,0)
        self.tables[name] = table
        return table

    def get_table(self, name):
        with open(os.getcwd() + '/' + name + '/metadata.json', 'r') as fp:
            meta_dict = json.load(fp)
        fp.close()
        table = Table(name, meta_dict['num_column'], meta_dict['key'], self.bufferpool, meta_dict['num_basepage'],
                      meta_dict['num_tailpage'], meta_dict['total_records'])
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        del self.tables[name]
        pass
