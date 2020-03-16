from lstore.page import *
from lstore.index import Index
from lstore.bufferpool import Bufferpool
from lstore.mylock import MyLock
import threading
from time import sleep


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        pass

    def __str__(self):
        return str(self.columns)


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_records = 0
        self.page_directory = {}
        self.base_updates = []
        self.bufferpool = Bufferpool()
        self.index = Index(self)
        self.lock_manage = MyLock()
        merge_thread = threading.Thread(target=self.__merge)
        merge_thread.daemon = True
        merge_thread.start()
        pass

    def __merge(self):
        while True:
            num_base_pages = len(self.base_updates)
            for page_index in range(num_base_pages):
                if self.base_updates[page_index] == 0:
                    continue
                page_name = 'b_' + str(page_index) + '_c_' + str(self.key + 4)
                copy = self.get_from_disk(page_name)
                if copy.has_capacity():
                    continue
                if copy.tps == self.base_updates[page_index] - 1:
                    continue
                if self.base_updates[page_index] - copy.tps - 1 < MERGENUMBER:
                    continue
                for i in range(copy.tps, copy.tps+MERGENUMBER):
                    page_name = 't_' + str(page_index) + '_' + str(i//PAGESIZE) + '_c_' + str(RID_COLUMN)
                    rid_page = self.get_from_disk(page_name)
                    rid = rid_page.read(copy.tps % PAGESIZE)
                    base_slot = rid % PAGESIZE
                    for colunm in range(self.num_columns):
                        page_name = 't_' + str(page_index) + '_' + str(i//PAGESIZE) + '_c_' + str(colunm+4)
                        tail_page = self.get_from_disk(page_name)
                        updated_value = tail_page.read(copy.tps % PAGESIZE)
                        if updated_value == -1:
                            continue
                        page_name = 'b_' + str(page_index) + '_c_' + str(colunm+4)
                        base_page = self.get_from_disk(page_name)
                        base_page.modify(base_slot, updated_value)
                        base_page.tps = i
                        done = False
                        while not done:
                            done = self.bufferpool.replace(base_page)
                        sleep(MERGETIME)
        pass

    def get_from_disk(self, page_name):
        while not self.bufferpool.lock_buffer():
            sleep(0.5)
        for i in range(self.bufferpool.total_page):
            if self.bufferpool.pool[i].page_name == page_name:
                temp_page = self.bufferpool.pool[i]
                new_page = Page()
                new_page.page_name = page_name
                new_page.num_records = temp_page.num_records
                new_page.tps = temp_page.tps
                new_page.data = temp_page.data
                self.bufferpool.unlock_buffer()
                return new_page
        file = open(page_name, 'r')
        new_page = Page()
        new_page.page_name = page_name
        new_page.num_records = int(file.readline())
        new_page.tps = int(file.readline())
        new_page.data = eval(file.readline())
        file.close()
        self.bufferpool.unlock_buffer()
        return new_page
