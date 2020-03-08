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
        self.lock_manager = {}                      #{RID: (num_lock, num_xlock, [array of transaction holding lock]) } use Counter class
        self.bufferpool = bufferpool
        self.index = Index(self)
        self.start_merge = False
        pass

    def get_slock(self, rid, transaction):
        if len(self.lock_manager[rid] == 0):    
            self.table.lock_manage[rid] = (Counter() Counter(), [])
        (num_slock, num_xlock, transations) = self.lock_manager[rid]
        if num_xlock.value == 0:
            num_slock.inc()
            transations.append(transaction)
            transaction.locks_rid.append(rid)
            return True
        return False

    def get_xlock(self, rid, transaction):
        if len(self.lock_manager[rid] == 0):    #create lock if not exist(insert)
            self.table.lock_manage[rid] = (Counter() Counter(), [])
        (num_slock, num_xlock, transactions) = self.lock_manager[rid]
        if num_xlock.value == 0:
            if num_slock.value == 0:
                num_xlock.inc()
                transactions.append(transaction)
                transaction.locks_rid.append(rid)
                return True
            elif num_slock.value == 1 and len(transactions) == 1 and transactions[0] == transaction:
                #upgrade slock to xlock if it is the only lock holder
                num_xlock.inc()
                num_slock.dec()
                return True
        return False
    
    def release_s_lock(self, rid, transaction):
        (num_slock, num_xlock, transactions) = self.lock_manager[rid]
        num_slock.dec()
        transactions.remove(transation)
        transaction.locks_rid.remove(rid)
        return True

    def release_x_lock(self, rid, transaction):
        (num_slock, num_xlock, transaction) = self.lock_manager[rid]
        num_xlock.dec()
        transactions.remove(transation)
        transaction.locks_rid.remove(rid)
        return True

    def release_lock(self, rid, transaction):
        (num_slock, num_xlock, transaction) = self.lock_manager[rid]
        if transaction in transactions:
            if num_xlock == 1:
                self.release_xlock(rid,transaction)
            elif num_slock > 0:
                self.release_slock(rid,transaction)
                
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




 
