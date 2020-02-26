from lstore.page import *
from time import time
import os, sys, json

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    def __str__(self):
        return str(self.columns)

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferpool, num_basepage, num_tailpage,total_records):
        self.name = name
        self.disk_directory = os.getcwd() + "/" + self.name
        if not os.path.isdir(self.disk_directory):
            os.makedirs(self.disk_directory)
        self.key = key + 4
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.num_base_page = num_basepage
        self.num_tail_page = num_tailpage
        self.total_records = total_records
        if total_records % 512 == 0:
            self.page_full = True               
        else:
            self.page_full = False
        self.page_directory = {}            # A Python Dictionary in the format {RID:(Page_Index, Slot), ...}
        page_dict = self.disk_directory + "/page_dict.json"
        try:
            with open(page_dict, "x") as file:
                file.close()
        except FileExistsError:
            pass
        if os.stat(page_dict).st_size > 0:
            with open(page_dict, "r") as fp:
                f = json.load(fp)
                for i in range(0, len(f)):
                    page_index, slot = f[str(i)]
                    self.page_directory[i] = (page_index, slot)
            fp.close()

    def write_meta_to_disk(self):
        meta_dict = {}
        meta_dict.update({'key': self.key-4})
        meta_dict.update({'num_column': self.num_columns})
        meta_dict.update({'num_basepage': self.num_base_page})
        meta_dict.update({'num_tailpage': self.num_tail_page})
        meta_dict.update({'total_records': self.total_records})
        with open(self.disk_directory + '/metadata.json',"w") as fp:
            json.dump(meta_dict, fp)
        fp.close()
        
        with open(self.disk_directory + '/page_dict.json', "w") as fp:
            json.dump(self.page_directory, fp)
        fp.close()

    def __merge(self):
        pass
 
