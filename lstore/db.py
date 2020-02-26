from lstore.table import Table
from lstore.page import *
from collections import defaultdict
from collections import deque
import os, sys, json
import numpy as np

class Bufferpool:

    def __init__(self):
        self.empty = [i for i in range(90)]
        self.used = deque()
        self.pinned = np.zeros(90, dtype=int)
        self.LRUIndex = np.empty(90, dtype=int)
        self.pool = [Page("", ()) for i in range(90)]  # empty page, will be replace by real page
        self.directory = defaultdict(lambda: -1)  # (table, b or t, page_index, column_number) -> index in pool
        # -1 if not in pool(need to get from file)

    def flush_pool(self):
        for page in self.pool:
            if page.dirty and page.num_records > 0:
                #print("writing page to file, filename = " + str(page.filename) + " meta = " + str(page.meta) + " num_records = " + str(page.num_records) + " dirty = " + str(page.dirty))
                txtfile = open(page.filename, "w")
                txtfile.write(str(page.lineage) + '\n')
                data_str = ""
                for i in range(page.num_records):
                    data_str += str(int.from_bytes(page.read(i), 'big')) + " "
                txtfile.write(data_str)
                txtfile.close()

    def info_page(self,index):
        page = self.pool[index]
        print("pool[" +str(index) + "]: filename = " + str(page.filename) + " meta = " + str(page.meta) + " num_records = " + str(page.num_records) + " dirty = " + str(page.dirty))

    def info_pool(self):
        print("information of pages in bufferpool::")
        for i in range(90):
            if self.pool[i].num_records > 0:
                page = self.pool[i]
                print("pool[" +str(i) + "]: filename = " + str(page.filename) + " meta = " + str(page.meta) + " num_records = " + str(page.num_records) + " dirty = " + str(page.dirty))

    def dirty_pool(self):
        print("Dirty pages in bufferpool::")
        for i in range(90):
            if self.pool[i].dirty:
                page = self.pool[i]
                print("pool[" +str(i) + "]: filename = " + str(page.filename) + " meta = " + str(page.meta) + " num_records = " + str(page.num_records) + " dirty = " + str(page.dirty))

    def get_from_disk(self, filename, meta):
        if os.path.isfile(filename) and os.stat(filename).st_size > 0:
            txtfile = open(filename, "r")
            file_lines = txtfile.readlines()
            data_list = file_lines[1].split()
            txtfile.close()
            data = []
            for i in data_list:
                i_int = int(i)
                data.append(i_int)
            page = Page(filename, meta)
            page.lineage = int(file_lines[0])
            for i in range(len(data)):
                page.write(data[i].to_bytes(8, 'big'))
            #print("page got from disk: filename = " + str(page.filename) + " meta = " + str(page.meta))
            #print("page has " + str(page.num_records) + " records")
        else:
            page = Page(filename, meta)
        page.dirty = False;
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.LRUIndex[empty_index] = len(self.used) - 1
        self.write(empty_index, page=page)
        #Sprint("page in pool: filename = " + str(self.pool[empty_index].filename) + " meta = " + str(self.pool[empty_index].meta))
        return empty_index

    def write(self, index, page=-1, value=-1):
        if(page != -1):
            self.pool[index] = page
        if value != -1:
            self.pool[index].write(value)
        #self.pinned[index] = self.pinned[index] - 1
    def get(self, index):
        # self.pinned[index] = self.pinned[index] - 1
        return self.pool[index]

    def evict(self):
        evict_index = 0
        for i in range(0, len(self.used)):  # Evict the least recently used and unpinned page
            if (self.pinned[self.used[i]] == 0):
                evict_index = self.used.popleft()
                self.LRUIndex[evict_index] = -1
                break
        # evict oldest page
        self.empty.append(evict_index)
        if self.pool[evict_index].dirty:
            page = self.pool[evict_index]
            txtfile = open(page.filename, "w")
            txtfile.write(str(page.lineage) + '\n')
            data_str = ""
            for i in range(page.num_records):
                data_str += str(int.from_bytes(page.read(i), 'big')) + " "
            txtfile.write(data_str)
            txtfile.close()
        del self.directory[self.pool[evict_index].meta]

    def getindex(self, table, bt, page_index, page_number, column):
        i = self.directory[(table, bt, page_index, page_number, column)]
        if i == -1:
            if len(self.used) == len(self.pool):
                self.evict()
            path = os.getcwd() + "/" + table + "/" + bt + "_" + str(page_index) + "p_" + str(page_number) + "c_" + str(column)
            i = self.get_from_disk(path, (table, bt, page_index, page_number, column))
            self.directory.update({(table, bt, page_index, page_number, column): i})
        self.used.__delitem__(self.LRUIndex[i])
        self.used.append(i)
        self.LRUIndex[i] = len(self.used) - 1
        # self.pinned[i] = self.pinned[i] + 1
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
        table = Table(name, num_columns, key, self.bufferpool,0,0,0,0)
        self.tables.update({name: table})
        return table

    def get_table(self, name):
        with open(os.getcwd() + '/' + name + '/metadata.json', 'r') as fp:
            meta_dict = json.load(fp)
        fp.close()
        table = Table(name, meta_dict['num_column'], meta_dict['key'], self.bufferpool, meta_dict['num_basepage'],
                      meta_dict['num_tailpage'], meta_dict['total_base_records'], meta_dict['total_tail_records'])
        print(table.name, table.num_columns, table.key, table.num_base_page, table.num_tail_page, table.total_base_records, table.total_tail_records)
        self.tables.update({name: table})
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        del self.tables[name]
        pass
