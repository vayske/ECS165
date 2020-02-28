from lstore.page import *

CAPACITY = 90

class Bufferpool:

    def __init__(self):
        self.total_page = 0
        self.pool = []

    def write(self, page_name, value):
        index = self.get_page(page_name)
        self.pool[index].write(value)

    def read(self, page_name, slot):
        index = self.get_page(page_name)
        return self.pool[index].read(slot)

    def change_value(self, page_name, slot, value):
        index = self.get_page(page_name)
        self.pool[index].change_value(slot, value)

    def get_page(self, page_name):
        done = False
        for i in range(0, self.total_page):
            if self.pool[i].page_name == page_name:
                #self.pool[i].used += 1
                done = True
                return i
        if not done:
            new_page = Page()
            try:
                with open(page_name, "x") as file:
                    file.close()
                    new_page.page_name = page_name
            except FileExistsError:
                file = open(page_name, "r")
                new_page.page_name = page_name
                new_page.num_records = int(file.readline())
                new_page.data = eval(file.readline())
                file.close()
            if(self.total_page >= CAPACITY):
                self.evict()
                self.pool.append(new_page)
                self.total_page += 1
                return self.total_page - 1
            else:
                self.pool.append(new_page)
                self.total_page += 1
                return self.total_page - 1

    def evict(self):
        #least_used = self.pool[0].used
        position = 0
        for i in range(0, self.total_page):
            if not self.pool[i].merging and not self.pool[i].pin:
                #least_used = self.pool[i].used
                position = i
                break
        if self.pool[position].dirty:
            file = open(self.pool[position].page_name, "w")
            file.write(str(self.pool[position].num_records) + "\n")
            file.write(str(self.pool[position].data) + "\n")
            file.close()
        self.pool.pop(position)
        self.total_page -= 1

    def flush(self):
        for i in range(0, self.total_page):
            if self.pool[i].dirty:
                file = open(self.pool[i].page_name, "w")
                file.write(str(self.pool[i].num_records) + "\n")
                file.write(str(self.pool[i].data) + "\n")
                file.close()

    def replace_page(self, page):
        position = self.get_page(page.name)
        if self.pool[position].pin:
            return False
        else:
            self.pool[position] = page
            self.pool[position].dirty = True
            return True





