from lstore.page import *
from lstore.index import Index
from time import time
from lstore.bufferpool import Bufferpool
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
        self.page_directory = {}
        self.lock_manager = {}                      #{RID: (num_lock, num_xlock)} use Counter class
        self.bufferpool = bufferpool
        self.index = Index(self)
        self.start_merge = False
        pass

    def get_slock(self, rid):
        (num_slock, num_xlock) = self.lock_manager[rid]
        if num_xlock.value == 0:
            num_slock.inc()
            return true
        return false

    def get_xlock(self, rid):
        (num_slock, num_xlock) = self.lock_manager[rid]
        if num_slock.value == 0 and num_xlock.value == 0:
            num_xlock.inc()
            return true
        return false
    
    def release_s_lock(self, rid):
        (num_slock, num_xlock) = self.lock_manager[rid]
        num_slock.dec()
        return true

    def release_x_lock(self, rid):
        (num_slock, num_xlock) = self.lock_manager[rid]
        num_xlock.dec()
        return true

    def merge(self, bufferpool):
        while(self.total_updates > 0):
            for i in range(0, self.total_updates):
                tail_index = i // 512
                tail_slot = i % 512
                for j in range(0, self.num_columns):
                    page_name = "t_" + str(tail_index) + "_" + "c_" + str((j+4))
                    updated_value = int.from_bytes(bufferpool.read(page_name, tail_slot), 'big', signed=True)
                    if updated_value == -1:
                        continue
                    page_name = "t_" + str(tail_index) + "_" + "c_" + str(RID_COLUMN)
                    base_rid = int.from_bytes(bufferpool.read(page_name, tail_slot), 'big', signed=True)
                    base_index, base_slot = self.page_directory[base_rid]
                    page_name = "b_" + str(base_index) + "_" + "c_" + str(j+4)
                    lock = threading.Lock()
                    lock.acquire()
                    page_index__in_pool = bufferpool.get_page(page_name)
                    copypage = bufferpool.pool[page_index__in_pool]
                    lock.release()
                    updated_value = updated_value.to_bytes(8, 'big', signed=True)
                    copypage.change_value(base_slot, updated_value)
                    while not bufferpool.replace_page(copypage):
                        pass




 
