from lstore.page import *
from lstore.index import Index
from time import sleep
from lstore.bufferpool import Bufferpool
import os
import threading
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
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_records = 0
        self.total_updates = 0
        self.lineage = 0
        self.page_directory = {}
        self.bufferpool = bufferpool
        self.index = Index(self)
        merge_thread = threading.Thread(target=self.__merge)
        merge_thread.daemon = True
        merge_thread.start()
        pass

    def __merge(self):
        while True:
            for i in range(self.lineage, self.total_updates):
                tail_index = i // 512
                tail_slot = i % 512
                page_name = "t_" + str(tail_index) + "_" + "c_" + str(RID_COLUMN)
                temp_page = self.read_from_disk(page_name)
                while temp_page is None:
                    temp_page = self.read_from_disk(page_name)
                base_rid = int.from_bytes(temp_page.read(tail_slot), 'big', signed=True)
                base_index, base_slot = self.page_directory[base_rid]
                for j in range(0, self.num_columns):
                    page_name = "t_" + str(tail_index) + "_" + "c_" + str((j+4))
                    temp_page = self.read_from_disk(page_name)
                    while temp_page is None:
                        temp_page = self.read_from_disk(page_name)
                    updated_value = int.from_bytes(temp_page.read(tail_slot), 'big', signed=True)
                    if updated_value == -1:
                        continue
                    page_name = "b_" + str(base_index) + "_" + "c_" + str((j+4))
                    temp_page = self.read_from_disk(page_name)
                    temp_page.change_value(base_slot, updated_value.to_bytes(8, 'big', signed=True))
                    temp_page.dirty = True
                    temp_page.TPS = i
                    self.write_to_disk(temp_page)
                    self.lineage += 1
                    sleep(0.5)

    def write_to_disk(self, page):
        file = open(page.page_name, "w")
        file.write(str(page.num_records) + "\n")
        file.write(str(page.TPS) + "\n")
        file.write(str(page.data) + "\n")
        file.close()

    def read_from_disk(self, page_name):
        if os.path.getsize(page_name) == 0:
            return None
        file = open(page_name, "r")
        new_page = Page()
        new_page.page_name = page_name
        new_page.num_records = int(file.readline())
        new_page.TPS = int(file.readline())
        new_page.data = eval(file.readline())
        file.close()
        return new_page




 
