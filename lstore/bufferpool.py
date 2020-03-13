from lstore.page import *
import threading
from time import sleep

class Bufferpool:

    def __init__(self):
        self.lock = threading.Lock()
        self.total_page = 0
        self.pool = []
        pass

    def get_page(self, page_name):
        while not self.lock.acquire():
            print('wait for merge\n')
        for i in range(self.total_page):
            if self.pool[i].page_name == page_name:
                self.lock.release()
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
            index = self.evict()
            self.pool.insert(index, new_page)
            self.lock.release()
            return index
        else:
            self.pool.append(new_page)
            index = self.total_page
            self.total_page += 1
            self.lock.release()
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
        return position

    def flush(self):
        for i in range(self.total_page):
            if self.pool[i].dirty:
                file = open(self.pool[i].page_name, 'w')
                file.write(str(self.pool[i].num_records) + '\n')
                file.write(str(self.pool[i].tps) + '\n')
                file.write(str(self.pool[i].data) + '\n')
                file.close()

    def replace(self, page):
        for i in range(self.total_page):
            if self.pool[i].page_name == page.page_name:
                if self.pool[i].pin > 0:
                    return False
                else:
                    self.pool[i] = page
                    return True
        file = open(page.page_name, 'w')
        file.write(str(page.num_records) + '\n')
        file.write(str(page.tps) + '\n')
        file.write(str(page.data) + '\n')
        file.close()
        return True