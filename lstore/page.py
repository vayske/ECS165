from lstore.config import *


class Page:

    def __init__(self, filename, meta):
        self.num_records = 0
        self.pin = 0
        self.dirty = False
        self.lineage = 0                                # b for base page t for tail page
        self.filename = filename                        #(os.cwd()/ + (b or t) + _(page_index) + p_(page_number) + c_(column_number))
        self.meta = meta                                #(table, b or t, page_index, page_number, column_number)
        self.data = bytearray(4096)

    def has_capacity(self):
        if(self.num_records == 512):                    # Since Everything is 64-bit Integer
            return False                                # The Max Capacity will be 512 Records
        return True
        

    # --- Value passed in is in Bytes --- #
    def write(self, value):
        next_index = self.num_records * 8
        self.data[next_index:next_index+8] = value[0:8]
        self.num_records = self.num_records + 1
        self.dirty = True
        pass

    def change_value(self, slot, value):
        index = slot * 8
        self.data[index:index+8] = value[0:8]
        self.dirty = True
        pass

    def read(self, slot):
        index = slot * 8
        value = self.data[index:index+8]
        return value