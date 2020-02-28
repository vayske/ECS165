from lstore.config import *


class Page:

    def __init__(self):
        self.page_name = ""
        self.num_records = 0
        #self.used = 0
        self.TPS = -1
        self.dirty = False
        self.pin = False
        self.merging = False
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records >= 512:
            return False
        else:
            return True

    def write(self, value):
        self.pin = True
        position = self.num_records * 8
        self.data[position:position+8] = value
        self.num_records += 1
        self.dirty = True
        self.pin = False

    def change_value(self, slot, value):
        self.pin = True
        position = slot * 8
        self.data[position:position+8] = value
        self.dirty = True
        self.pin = False

    def read(self, slot):
        self.pin = True
        position = slot * 8
        self.pin = False
        return self.data[position:position+8]

