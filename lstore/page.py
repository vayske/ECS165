from lstore.config import *


class Page:

    def __init__(self):
        self.page_name = ""
        self.pin = 0
        self.dirty = False
        self.num_records = 0
        self.tps = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records > PAGESIZE:
            return False

    def write(self, value):
        self.pin += 1
        position = self.num_records * 8
        value_in_bytes = None
        if isinstance(value, str):
            value += '000'
            value_in_bytes = value.encode('utf-8')
        else:
            value_in_bytes = value.to_bytes(8, 'big', signed=True)
        self.data[position:position+8] = value_in_bytes
        self.num_records += 1
        self.dirty = True
        self.pin -= 1

    def read(self, slot, option=0):
        self.pin += 1
        position = slot * 8
        value_in_bytes = self.data[position:position+8]
        if option == 0:
            value = int.from_bytes(value_in_bytes, 'big', signed=True)
        else:
            schemacode = value_in_bytes.decode('utf-8')
            value = schemacode[0:5]
        self.pin -= 1
        return value

    def modify(self, slot, value):
        self.pin += 1
        position = slot * 8
        value_in_bytes = None
        if isinstance(value, str):
            value += '000'
            value_in_bytes = value.encode('utf-8')
        else:
            value_in_bytes = value.to_bytes(8, 'big', signed=True)
        self.data[position:position+8] = value_in_bytes
        self.dirty = True
        self.pin -= 1


