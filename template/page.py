from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if(self.num_records == 512):
            return False
        return True
        pass

    def write(self, value):
        next_index = self.num_records * 8
        self.data[next_index:next_index+8] = value[0:8]
        self.num_records += 1
        pass

    def change_value(self, slot, value):
        index = slot * 8
        self.data[index:index+8] = value[0:8]
        pass

    def read(self, slot):
        index = slot * 8
        value = self.data[index:index+8]
        return value