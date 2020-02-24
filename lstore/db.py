from lstore.table import Table
from lstore.page import *
from collections import defaultdict
import os, sys, json

class Bufferpool:

    def __init__(self,db):
        self.db = db
        self.empty = [i for i in range(90)]
        self.used = []
        self.pool = [Page("",()) for i in range(90)]                #empty page, will be replace by real page     
        self.directory = defaultdict(lambda:-1)                      #(table, b or t, page_index, column_number) -> index in pool
                                                                    # -1 if not in pool(need to get from file)
    def flush_pool(self):
        for page in self.pool:
            if page.dirty:
                file = open(page.filename, "wb")
                data_str = page.data
                file.write(data_str)
                file.close()
    
    def get_from_disk(self, filename, meta):
        if os.stat(filename).st_size >0:
            file=open(filename,"rb")
            data_str = file.read()
            page = Page(filename,meta)
            page.data = data_str
        else:
            page = Page(filename,meta)
        
        page.dirty = False;
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.pool[empty_index] = page
        return empty_index
    
    def evict(self):
        evict_index = self.used.pop(0)                                #evict oldest page
        self.empty.append(evict_index)
        if self.pool[evict_index].dirty:
            page = self.pool[evict_index]
            file = open(page.path, "wb")
            data_str = page.data
            file.write(data_str)
            file.close()
        del self.directory[page.location]
    
    def getindex(self, table, bt, page, column):
        i = self.directory[(table,bt,page,column)]
        if i == -1:
            if len(self.used) == len(self.pool):
                self.evict()
        path = os.getcwd() + "/" + bt + "_" + str(page) + "c_" +str(column)
        i = self.get_from_disk(path, (table, bt, page, column))
        self.directory.update({(table, bt, page, column): i})
        return i

class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = Bufferpool(self)
        pass

    def open(self,path):
        if not os.path.exist(path):
            os.makedirs(path)
        os.chdir(path)
        

    def close(self):
        for table in self.tables.items():
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
        table = Table(name,  num_columns, key, self.bufferpool)
        self.tables.update({name: table})
        return table

    def get_table(self, name):
        with open(os.getcwd()+'/'+name+'/metadata.json','r') as fp:
            meta_dict = json.load(fp.read())
        fp.close()
        table = Table(name, meta_dict['num_column'], meta_dict['key'], self.bufferpool, meta_dict['num_basepage'], meta_dict['num_tailpage'], meta_dict['total_records'])
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name]
        pass
