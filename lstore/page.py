from lstore.config import *
from lstore.counter import *

class Page:

    def __init__(self):
        self.page_name = ""
        self.num_records = 0
        #self.used = 0
        self.dirty = False
        self.pin = Counter()
        self.merging = False
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records >= 512:
            return False
        else:
            return True

    def write(self, value):
        self.pin.inc()
        position = self.num_records * 8
        self.data[position:position+8] = value
        self.num_records += 1
        self.dirty = True
        self.pin.dec()

    def change_value(self, slot, value):
        self.pin.inc()
        position = slot * 8
        self.data[position:position+8] = value
        self.dirty = True
        self.pin.dec()

    def read(self, slot):
        self.pin.inc()
        position = slot * 8
        self.pin.dec()
        return self.data[position:position+8]

