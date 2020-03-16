from lstore.page import *
import threading
from time import sleep


class Bufferpool:

    def __init__(self):
        self.lock = 0
        self.total_page = 0
        self.pool = []
        pass

    def write(self, page_name, value):
        while not self.lock_buffer():
            sleep(0.05)
        index = self.get_page(page_name)
        self.pool[index].write(value)
        self.unlock_buffer()

    def read(self, page_name, slot, option=0, tps=False):
        while not self.lock_buffer():
            sleep(0.05)
        index = self.get_page(page_name)
        value = self.pool[index].read(slot, option)
        TPS = self.pool[index].tps
        self.unlock_buffer()
        if tps:
            return value, TPS
        else:
            return value

    def modify(self, page_name, slot, value):
        while not self.lock_buffer():
            sleep(0.01)
        index = self.get_page(page_name)
        self.pool[index].modify(slot, value)
        self.unlock_buffer()

    def lock_buffer(self):
        if self.lock > 0:
            return False
        else:
            self.lock += 1
            return True

    def unlock_buffer(self):
        self.lock -= 1
        return True

    def get_page(self, page_name):
        for i in range(self.total_page):
            if self.pool[i].page_name == page_name:
                return i
        new_page = Page()
        try:
            with open(page_name, 'x') as file:
                file.close()
                new_page.page_name = page_name
        except FileExistsError:
            file = open(page_name, 'r')
            new_page.page_name = page_name
            new_page.num_records = int(file.readline())
            new_page.tps = int(file.readline())
            new_page.data = eval(file.readline())
            file.close()
        if self.total_page >= POOLSIZE:
            self.evict()
            self.pool.append(new_page)
            index = self.total_page
            self.total_page += 1
            return index
        else:
            self.pool.append(new_page)
            index = self.total_page
            self.total_page += 1
            return index

    def evict(self):
        position = 0
        for i in range(self.total_page):
            if self.pool[i].pin == 0:
                position = i
                break
        if self.pool[position].dirty:
            file = open(self.pool[position].page_name, 'w')
            file.write(str(self.pool[position].num_records) + '\n')
            file.write(str(self.pool[position].tps) + '\n')
            file.write(str(self.pool[position].data) + '\n')
            file.close()
        self.pool.pop(position)
        self.total_page -= 1

    def flush(self):
        while not self.lock_buffer():
            sleep(0.05)
        for i in range(self.total_page):
            if self.pool[i].dirty:
                file = open(self.pool[i].page_name, 'w')
                file.write(str(self.pool[i].num_records) + '\n')
                file.write(str(self.pool[i].tps) + '\n')
                file.write(str(self.pool[i].data) + '\n')
                file.close()
        self.unlock_buffer()

    def replace(self, page):
        while not self.lock_buffer():
            return False
        for i in range(self.total_page):
            if self.pool[i].page_name == page.page_name:
                if self.pool[i].pin > 0:
                    self.unlock_buffer()
                    return False
                else:
                    self.pool.pop(i)
                    self.pool.append(page)
                    self.unlock_buffer()
                    return True
        file = open(page.page_name, 'w')
        file.write(str(page.num_records) + '\n')
        file.write(str(page.tps) + '\n')
        file.write(str(page.data) + '\n')
        file.close()
        self.unlock_buffer()
        return True
